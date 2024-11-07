package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"net"
	"os"
	"sync"
	"dfs/generated/dfspb" // Import the generated protobuf code
	"google.golang.org/protobuf/proto"
)

type Client struct {
	ControllerAddr string
}

// Handle file upload
func (c *Client) handlePut(filename string, chunkSize int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Failed to get file info: %v", err)
	}

	if fileInfo.IsDir() {
		log.Fatalf("Cannot store directories")
	}

	// Request storage allocation from the controller
	conn, err := net.Dial("tcp", c.ControllerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Send ALLOCATE_STORAGE request
	allocateReq := &dfspb.Request{
		Type: dfspb.RequestType_ALLOCATE_STORAGE,
		Request: &dfspb.Request_AllocateStorage{
			AllocateStorage: &dfspb.AllocateStorageRequest{
				Filename: fileInfo.Name(),
				FileSize: fileInfo.Size(),
				ChunkNum: int64((fileInfo.Size() + int64(chunkSize) - 1) / int64(chunkSize)), // Calculate chunk number
			},
		},
	}

	if err := c.sendRequest(conn, allocateReq); err != nil {
		log.Fatalf("Failed to send allocate storage request: %v", err)
	}
	log.Printf("allocation request send", allocateReq.String())
	// Handle allocation response
	resp, err := c.readResponse(conn)
	if err != nil {
		log.Fatalf("Failed to read allocation response: %v", err)
	}

	allocations := resp.GetAllocateStorage().ChunkAllocations
	if allocations == nil {
		log.Fatalf("Failed to allocate storage: %v", resp.GetAllocateStorage().ErrorMessage)
	}

	// Read file and store chunks based on allocation
	chunks := c.chunkFile(file, chunkSize)
	for i, chunk := range chunks {
		// Calculate checksum for the chunk
		checksum := c.calculateChecksum(chunk)

		// Get the storage node address for this chunk
		if i >= len(allocations) {
			log.Fatalf("Chunk allocation missing for chunk %d", i)
		}
		alloc := allocations[i]

		// Check if there's at least one node allocated for this chunk
		if len(alloc.Nodes) == 0 {
			log.Fatalf("No storage node allocated for chunk %d", i)
		}

		storageAddr := alloc.Nodes[0].Address

		// Connect to the storage node
		storageConn, err := net.Dial("tcp", storageAddr)
		if err != nil {
			log.Fatalf("Failed to connect to storage node %s: %v", storageAddr, err)
		}
		defer storageConn.Close()

		// Send StoreChunkRequest
		storeReq := &dfspb.Request{
			Type: dfspb.RequestType_STORE_CHUNK,
			Request: &dfspb.Request_StoreChunk{
				StoreChunk: &dfspb.StoreChunkRequest{
					ChunkId:  fmt.Sprintf("%s_%d", fileInfo.Name(), i),
					Data:     chunk,
					Checksum: checksum,
					Nodes: alloc.Nodes[1:],
				},
			},
		}

		if err := c.sendRequest(storageConn, storeReq); err != nil {
			log.Fatalf("Failed to send store chunk request: %v", err)
		}

		// Handle the response from the storage node
		storeResp, err := c.readResponse(storageConn)
		if err != nil {
			log.Fatalf("Failed to read response from storage node: %v", err)
		}

		if !storeResp.GetStoreChunk().Success {
			log.Fatalf("Failed to store chunk %d: %v", i, storeResp.GetStoreChunk().ErrorMessage)
		}

		log.Printf("Stored chunk %d successfully", i)
	}
}

// Chunk file into pieces
func (c *Client) chunkFile(file *os.File, chunkSize int) [][]byte {
	var chunks [][]byte
	buffer := make([]byte, chunkSize)

	for {
		n, err := file.Read(buffer)
		if n > 0 {
			// Create a new slice with the exact data size for this chunk
			chunk := make([]byte, n)
			copy(chunk, buffer[:n])
			chunks = append(chunks, chunk)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read file: %v", err)
		}
	}

	return chunks
}


// Calculate checksum for a chunk of data
func (c *Client) calculateChecksum(data []byte) string {
	hash := sha256.New()
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// handleGet handles file retrieval in the client
func (c *Client) handleGet(filename string, storageDir string) {
	// Step 1: Connect to the controller to get the file chunk locations
	conn, err := net.Dial("tcp", c.ControllerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Step 2: Send the GetFileLocationsRequest to the controller
	fileLocReq := &dfspb.Request{
		Type: dfspb.RequestType_GET_FILE_LOCATIONS,
		Request: &dfspb.Request_GetFileLocations{
			GetFileLocations: &dfspb.GetFileLocationsRequest{
				Filename: filename,
			},
		},
	}

	// Send the request
	if err := c.sendRequest(conn, fileLocReq); err != nil {
		log.Fatalf("Failed to send file locations request: %v", err)
	}

	// Read the response from the controller
	resp, err := c.readResponse(conn)
	if err != nil {
		log.Fatalf("Failed to read file locations response: %v", err)
	}

	fileLocResp := resp.GetGetFileLocations()
	if fileLocResp.ErrorMessage != "" {
		log.Fatalf("Failed to retrieve file locations: %v", fileLocResp.ErrorMessage)
	}

	// Step 3: Open the file for writing (create/truncate)

	filePath := storageDir + "/" + filename

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create file %s: %v", filePath, err)
	}
	defer file.Close()

	// Step 4: Retrieve chunks in parallel and write them to the file
	numChunks := len(fileLocResp.ChunkLocations)
	chunkData := make([][]byte, numChunks)
	var wg sync.WaitGroup
	errs := make(chan error, numChunks) // To collect errors from goroutines

	// Launch goroutines to retrieve each chunk
	for i, chunkAlloc := range fileLocResp.ChunkLocations {
		wg.Add(1)
		go func(index int, alloc *dfspb.ChunkAllocation) {
			defer wg.Done()
			data, err := c.retrieveChunk(alloc)
			if err != nil {
				errs <- fmt.Errorf("Failed to retrieve chunk %d: %v", index, err)
				return
			}
			chunkData[index] = data
		}(i, chunkAlloc)
	}

	// Wait for all chunk retrievals to finish
	wg.Wait()
	close(errs)

	// Check for errors from any goroutine
	for err := range errs {
		if err != nil {
			log.Fatalf("Error during chunk retrieval: %v", err)
		}
	}

	// Step 5: Write chunks to the file in order
	for _, data := range chunkData {
		if _, err := file.Write(data); err != nil {
			log.Fatalf("Failed to write chunk to file: %v", err)
		}
	}

	log.Printf("Successfully retrieved and wrote file: %s", filename)
}

// Helper function to retrieve a chunk from a storage node
func (c *Client) retrieveChunk(chunkAlloc *dfspb.ChunkAllocation) ([]byte, error) {
	// Connect to the first available storage node for the chunk
	storageNode := chunkAlloc.Nodes[0] // Simplifying by picking the first node
	conn, err := net.Dial("tcp", storageNode.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to storage node %s: %v", storageNode.NodeId, err)
	}
	defer conn.Close()

	// Create the RetrieveChunkRequest
	chunkReq := &dfspb.Request{
		Type: dfspb.RequestType_RETRIEVE_CHUNK,
		Request: &dfspb.Request_RetrieveChunk{
			RetrieveChunk: &dfspb.RetrieveChunkRequest{
				ChunkId: chunkAlloc.ChunkId,
			},
		},
	}

	// Send the request
	if err := c.sendRequest(conn, chunkReq); err != nil {
		return nil, fmt.Errorf("failed to send chunk retrieve request: %v", err)
	}

	// Read the response
	resp, err := c.readResponse(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk retrieve response: %v", err)
	}

	// Get the RetrieveChunkResponse
	chunkResp := resp.GetRetrieveChunk()
	if chunkResp.ErrorMessage != "" {
		return nil, fmt.Errorf("failed to retrieve chunk: %s", chunkResp.ErrorMessage)
	}

	// Step 6: Verify the checksum of the retrieved chunk
	if !c.verifyChecksum(chunkResp.Data, chunkResp.Checksum) {
		return nil, fmt.Errorf("checksum verification failed for chunk %s", chunkAlloc.ChunkId)
	}

	return chunkResp.Data, nil
}

// Helper function to verify chunk checksum
func (c *Client) verifyChecksum(data []byte, expectedChecksum string) bool {
	// Assuming we're using some checksum function like SHA-256 for the verification
	actualChecksum := c.calculateChecksum(data) // Implement this function
	return actualChecksum == expectedChecksum
}

// handleList handles listing files in the distributed file system
func (c *Client) handleList() {
	// Connect to the controller
	conn, err := net.Dial("tcp", c.ControllerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create the ListFilesRequest
	listFilesReq := &dfspb.Request{
		Type: dfspb.RequestType_LIST_FILES,
		Request: &dfspb.Request_ListFiles{
			ListFiles: &dfspb.ListFilesRequest{},
		},
	}

	// Send the request
	if err := c.sendRequest(conn, listFilesReq); err != nil {
		log.Fatalf("Failed to send list files request: %v", err)
	}

	// Read the response
	resp, err := c.readResponse(conn)
	if err != nil {
		log.Fatalf("Failed to read list files response: %v", err)
	}

	// Process the response
	listFilesResp := resp.GetListFiles()
	if listFilesResp.ErrorMessage != "" {
		log.Fatalf("Failed to list files: %v", listFilesResp.ErrorMessage)
	}

	log.Println("Files stored in the system:")
	for _, file := range listFilesResp.Filenames {
		fmt.Println(file)
	}
}

// handleSystemInfo handles retrieving system information
func (c *Client) handleSystemInfo() {
	// Connect to the controller
	conn, err := net.Dial("tcp", c.ControllerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create the GetSystemInfoRequest
	sysInfoReq := &dfspb.Request{
		Type: dfspb.RequestType_GET_SYSTEM_INFO,
		Request: &dfspb.Request_GetSystemInfo{
			GetSystemInfo: &dfspb.GetSystemInfoRequest{},
		},
	}

	// Send the request
	if err := c.sendRequest(conn, sysInfoReq); err != nil {
		log.Fatalf("Failed to send system info request: %v", err)
	}

	// Read the response
	resp, err := c.readResponse(conn)
	if err != nil {
		log.Fatalf("Failed to read system info response: %v", err)
	}

	// Process the response
	sysInfoResp := resp.GetGetSystemInfo()
	if sysInfoResp.ErrorMessage != "" {
		log.Fatalf("Failed to retrieve system info: %v", sysInfoResp.ErrorMessage)
	}

	// Display system information
	log.Println("System Information:")
	fmt.Println("Active Nodes:")
	for _, node := range sysInfoResp.ActiveNodes {
		fmt.Printf("Node ID: %s, Address: %s, Free Space: %d MB, Total Requests: %d\n",
			node.NodeId, node.Address, node.FreeSpace, node.TotalRequests)
	}	
}

func (c *Client) handleDelete(filename string) {
	// Step 1: Connect to the controller
	conn, err := net.Dial("tcp", c.ControllerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Step 2: Create and send the DeleteFileRequest to the controller
	deleteFileReq := &dfspb.Request{
		Type: dfspb.RequestType_DELETE_FILE,
		Request: &dfspb.Request_DeleteFile{
			DeleteFile: &dfspb.DeleteFileRequest{
				Filename: filename,
			},
		},
	}

	if err := c.sendRequest(conn, deleteFileReq); err != nil {
		log.Fatalf("Failed to send delete file request: %v", err)
	}

	// Step 3: Receive the DeleteFileResponse from the controller
	resp, err := c.readResponse(conn)
	if err != nil {
		log.Fatalf("Failed to read delete file response: %v", err)
	}

	deleteFileResp := resp.GetDeleteFile()
	if deleteFileResp.ErrorMessage != "" {
		log.Fatalf("Failed to delete file: %v", deleteFileResp.ErrorMessage)
	}

	// Step 4: Retrieve chunk allocations for the file and send DeleteChunkRequests to storage nodes
	var wg sync.WaitGroup

	for _, chunk := range deleteFileResp.ChunkLocations {
		for _, node := range chunk.Nodes {
			wg.Add(1)  // Increment the WaitGroup counter
			go func(chunkId, nodeAddress string) {
				defer wg.Done()  // Decrement the counter when the goroutine completes
				c.deleteChunkFromNode(chunkId, nodeAddress)
			}(chunk.ChunkId, node.Address)
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Printf("File '%s' deleted successfully", filename)
}

// Helper function to delete a chunk from a storage node
func (c *Client) deleteChunkFromNode(chunkID, nodeAddr string) {
	// Step 5: Connect to the storage node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Printf("Failed to connect to storage node (%s): %v", nodeAddr, err)
		return
	}
	defer conn.Close()

	// Step 6: Create and send the DeleteChunkRequest to the storage node
	deleteChunkReq := &dfspb.Request{
		Type: dfspb.RequestType_DELETE_CHUNK,
		Request: &dfspb.Request_DeleteChunk{
			DeleteChunk: &dfspb.DeleteChunkRequest{
				ChunkId: chunkID,
			},
		},
	}

	if err := c.sendRequest(conn, deleteChunkReq); err != nil {
		log.Printf("Failed to send delete chunk request for chunk %s: %v", chunkID, err)
		return
	}

	// Step 7: Receive the DeleteChunkResponse from the storage node
	resp, err := c.readResponse(conn)
	if err != nil {
		log.Printf("Failed to read delete chunk response for chunk %s: %v", chunkID, err)
		return
	}

	deleteChunkResp := resp.GetDeleteChunk()
	if !deleteChunkResp.Success {
		log.Printf("Error deleting chunk %s: %v", chunkID, deleteChunkResp.ErrorMessage)
	} else {
		log.Printf("Successfully deleted chunk %s from node %s", chunkID, nodeAddr)
	}
}


// Send a protobuf request over the connection
func (c *Client) sendRequest(conn net.Conn, req *dfspb.Request) error {
	// Marshal the request to protobuf format
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	// Write the length of the message
	length := uint32(len(data))
	if err := binary.Write(conn, binary.BigEndian, length); err != nil {
		return err
	}

	// Write the message data
	_, err = conn.Write(data)
	return err
}

// Read a protobuf response from the connection
func (c *Client) readResponse(conn net.Conn) (*dfspb.Response, error) {
	// Assuming there's a predefined way to determine the message length
	var length int32
	err := binary.Read(conn, binary.BigEndian, &length)
	if err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var resp dfspb.Response
	if err := proto.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &resp, nil
}

func main() {

	client := &Client{
		ControllerAddr: "orion01:5002", // Replace with actual controller address
	}

	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <action> [<file>] [chunksize] [storageDir]", os.Args[0])
	}

	action := os.Args[1]

	switch strings.ToLower(action) {
	case "put":
		filename := os.Args[2]
		chunkSize := 6// Default chunk size is 10 MB
		if len(os.Args) == 4 {
			size, err := strconv.Atoi(os.Args[3])
			if err != nil || size <= 0 {
				log.Fatalf("Invalid chunk size: %v", err)
			}
			chunkSize = size * 1024*1024
		}
		client.handlePut(filename, chunkSize)
	case "get":

		filename := os.Args[2]
		storageDir := "."
	
		// Check if an optional storage server argument is provided
		if len(os.Args) > 3 {
			storageDir = os.Args[3]
			log.Printf("Using provided storage server: %s", storageDir)
		} else {
			log.Printf("Using default storage server: %s", storageDir)
		}
	
		client.handleGet(filename, storageDir)	
	case "delete":
		if len(os.Args) < 3 {
			log.Fatalf("Usage for delete: %s delete <file>", os.Args[0])
		}
		filename := os.Args[2]
		client.handleDelete(filename)
	case "list":
		client.handleList()
	case "system":
		client.handleSystemInfo()
	default:
		log.Fatalf("Invalid action: %s. Must be one of: put, get, delete, list, system", action)
	}
}