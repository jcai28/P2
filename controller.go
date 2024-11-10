package main

import (
	"encoding/binary"
	"io"
	"fmt"
	"log"
	"net"
	"sync"
	"math/rand"
	"time"
	"strings"
	"strconv"

	"github.com/golang/protobuf/proto"
	pb "dfs/generated/dfspb" // Import the proto package
)

// Controller struct will store the list of active storage nodes and file-to-chunk mappings
type Controller struct {
	StorageNodes   map[string]*pb.StorageNodeInfo   // Node ID to Node Info mapping
	NodesHeartbeat map[string]time.Time             // Last heartbeat times
	FileChunkMap   map[string][]*pb.ChunkAllocation // Filename to chunk locations
	NodeChunks     map[string][]string              // Node ID to chunk ID list
	ChunkFileMap   map[string]ChunkFileInfo         // Chunk ID to file and chunk number mapping
	nodeWorkload   map[string]int
	jobStatus       map[string]map[string]bool
	Mutex          sync.Mutex                       // To prevent race conditions
}

// ChunkFileInfo stores file name and chunk number for a given chunk ID
type ChunkFileInfo struct {
	Filename  string
	ChunkNum  int64
}

// NewController creates a new controller
func NewController() *Controller {
	return &Controller{
		StorageNodes:   make(map[string]*pb.StorageNodeInfo),
		NodesHeartbeat: make(map[string]time.Time),
		FileChunkMap:   make(map[string][]*pb.ChunkAllocation),
		NodeChunks:     make(map[string][]string),
		ChunkFileMap:   make(map[string]ChunkFileInfo), // Initialize the new map
		nodeWorkload:   make(map[string]int),
		jobStatus:      make(map[string]map[string]bool),
	}
}

// removeNodeFromAllocations handles the removal of a node from chunk allocations
func (c *Controller) removeNodeFromAllocations(nodeID string) {

	if chunks, exists := c.NodeChunks[nodeID]; exists {
		for _, chunkID := range chunks {
			chunkFileInfo, exists := c.ChunkFileMap[chunkID]
			if !exists {
				continue // If no mapping exists for this chunkID, skip
			}

			filename := chunkFileInfo.Filename
			index := chunkFileInfo.ChunkNum
			fmt.Printf("Removing for file %s, index %d\n", filename, index)

			
			// Access the chunk allocations for this filename
			chunkAllocations, exists := c.FileChunkMap[filename]
			
			if !exists {
				continue // If no allocations exist for this filename, skip
			}


			// Check if the index is valid
			if index < 0 || index >= int64(len(chunkAllocations)) {
				continue // Invalid index, skip
			}

			// Remove the node from the allocation's nodes list
			allocation := chunkAllocations[index]

			updatedNodes := []*pb.StorageNodeInfo{}
			for _, node := range allocation.Nodes {
				if node.NodeId != nodeID {
					updatedNodes = append(updatedNodes, node)
				}
			}

			// Create a new replica and add the target node to the updated list
			newNode := c.createNewReplica(chunkID, updatedNodes)
			if newNode != nil {
				updatedNodes = append(updatedNodes, newNode)
				c.NodeChunks[newNode.NodeId ] = append(c.NodeChunks[newNode.NodeId ], chunkID)
			}
			allocation.Nodes = updatedNodes

			fmt.Printf("Node %s removed from %s, %s\n", nodeID, filename, chunkID)
			fmt.Println(c.FileChunkMap[filename][index])
			
		}
		delete(c.NodeChunks, nodeID) // Remove the node from NodeChunks
	}
}

// containsNode checks if a node with the given ID exists in the list
func containsNode(nodes []*pb.StorageNodeInfo, nodeID string) bool {
	for _, node := range nodes {
		if node.NodeId == nodeID {
			return true
		}
	}
	return false
}

func (c *Controller) createNewReplica(chunkID string, nodes []*pb.StorageNodeInfo) *pb.StorageNodeInfo {
	// Check if there are nodes available to create a new replica
	if len(nodes) == 0 {
		fmt.Println("No nodes available to create a new replica.")
		return nil
	}

	sourceNode := nodes[0]

	var targetNode *pb.StorageNodeInfo

	// Iterate through the StorageNodes map to find a suitable target node
	for _, node := range c.StorageNodes {
		if !containsNode(nodes, node.NodeId) {
			targetNode = node
			break
		}
	}

	// If no target node was found, return nil
	if targetNode == nil {
		fmt.Println("No available target node to create a new replica.")
		return nil
	}

	// Establish a connection to the source node that has the chunk data
	conn, err := net.Dial("tcp", sourceNode.Address)
	if err != nil {
		fmt.Printf("Failed to connect to node %s: %v\n", sourceNode.NodeId, err)
		return nil
	}
	defer conn.Close()

	// Create the AddReplicaRequest
	request := &pb.Request{
		Type: pb.RequestType_ADD_REPLICA, // Set the request type to ADD_REPLICA
		Request: &pb.Request_AddReplica{
			AddReplica: &pb.AddReplicaRequest{
				ChunkId:   chunkID,
				TargetNode: targetNode, // The target node where the new replica should be created
			},
		},
	}

	// Send the request to create a new replica
	c.sendRequest(conn, request)

	fmt.Printf("New replica request sent to node %s to create replica in %s\n", sourceNode.NodeId, targetNode.NodeId)
	// Return the target node for further use if needed
	return targetNode
}

// MonitorNodes periodically checks the heartbeat of storage nodes
func (c *Controller) MonitorNodes(heartbeatTimeout time.Duration) {
	for {
		time.Sleep(1 * time.Second) // Check every second

		c.Mutex.Lock()
		currentTime := time.Now()

		for nodeID, lastHeartbeat := range c.NodesHeartbeat {
			if currentTime.Sub(lastHeartbeat) > heartbeatTimeout {
				// Remove the node from the maps after missing heartbeats
				delete(c.NodesHeartbeat, nodeID)
				delete(c.StorageNodes, nodeID) 
				fmt.Printf("Node %s missed heartbeats, removing from active nodes\n", nodeID)

				// Call the function to handle node removal from allocations
				c.removeNodeFromAllocations(nodeID)
			}
		}
		c.Mutex.Unlock()
	}
}


// handleHeartbeat handles incoming heartbeat requests from storage nodes
func (c *Controller) handleHeartbeat(req *pb.HeartbeatRequest, conn net.Conn) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Update the node info and record the latest heartbeat time
	c.StorageNodes[req.NodeInfo.NodeId] = req.NodeInfo
	c.NodesHeartbeat[req.NodeInfo.NodeId] = time.Now() // Corrected: changed eq to req

	// Send a response back to acknowledge the heartbeat
	resp := &pb.Response{
		Type: pb.RequestType_HEARTBEAT,
		Response: &pb.Response_Heartbeat{
			Heartbeat: &pb.HeartbeatResponse{
				Acknowledged: true,
			},
		},
	}
	c.sendResponse(conn, resp)
}


func (c *Controller) startStorageNodeServer(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start storage node server: %v", err)
	}
	defer listener.Close()

	log.Printf("Controller listening for storage nodes on port %s...", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept storage node connection: %v", err)
			continue
		}
		go c.handleStorageNodeConnection(conn) // Handle storage node connections
	}
}

func (c *Controller) startClientServer(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start client server: %v", err)
	}
	defer listener.Close()

	log.Printf("Controller listening for clients on port %s...", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept client connection: %v", err)
			continue
		}
		go c.handleClientConnection(conn) // Handle client connections
	}
}

func (c *Controller) handleStorageNodeConnection(conn net.Conn) {
	defer conn.Close()

	// Read the incoming message
	req, err := c.readRequest(conn)
	if err != nil {
		log.Printf("Failed to read storage node request: %v", err)
		return
	}
	switch req.Type {
	case pb.RequestType_HEARTBEAT:
		c.handleHeartbeat(req.GetHeartbeat(), conn)
	case pb.RequestType_REPORT_CORRUPTION:
		c.handleReportCorruption(req.GetReportCorruptedChunk(), conn)
	// Handle other client request types...
	default:
		log.Printf("Unknown request type from storage node")
	}
		
}

func (c *Controller) handleReportCorruption(req *pb.ReportCorruptedChunkRequest, conn net.Conn) {
    chunkId := req.ChunkId
    nodeId := req.NodeId

    log.Printf("Received report of corruption for chunk %s from node %s", chunkId, nodeId)

    // Acknowledge the report
    ackResp := &pb.ReportCorruptedChunkResponse{
        Acknowledged: true,
        ErrorMessage: "", // No error
    }

    // Split the chunkId to retrieve filename and index
    parts := strings.Split(chunkId, "_")
    if len(parts) != 2 {
        ackResp.Acknowledged = false
        ackResp.ErrorMessage = "Invalid chunkId format"
        response := &pb.Response{
			Type: pb.RequestType_REPORT_CORRUPTION,
			Response: &pb.Response_ReportCorruptedChunk{
				ReportCorruptedChunk: ackResp,
			},
		}
		c.sendResponse(conn, response)
        return
    }

    // Extract filename and index
    filename := parts[0]
    index, err := strconv.ParseInt(parts[1], 10, 64)
    if err != nil || index < 0 {
        ackResp.Acknowledged = false
        ackResp.ErrorMessage = "Invalid index"
		response := &pb.Response{
			Type: pb.RequestType_REPORT_CORRUPTION,
			Response: &pb.Response_ReportCorruptedChunk{
				ReportCorruptedChunk: ackResp,
			},
		}
		c.sendResponse(conn, response)
        return
    }

    // Access the chunk allocations for this filename
    allocations, exists := c.FileChunkMap[filename]
    if !exists || index >= int64(len(allocations)) {
        ackResp.Acknowledged = false
        ackResp.ErrorMessage = "No allocation found for the given filename and index"
		response := &pb.Response{
			Type: pb.RequestType_REPORT_CORRUPTION,
			Response: &pb.Response_ReportCorruptedChunk{
				ReportCorruptedChunk: ackResp,
			},
		}
		c.sendResponse(conn, response)
        return
    }

    allocation := allocations[index]
    var healthyNode *pb.StorageNodeInfo

    // Find a healthy replica node
    for _, node := range allocation.Nodes {
        if node.NodeId != nodeId {
            healthyNode = node
            break
        }
    }

    // If no healthy nodes were found, set the error message
    if healthyNode == nil {
        ackResp.Acknowledged = false
        ackResp.ErrorMessage = "No healthy replicas available"
    } else {
        ackResp.HealthyNode = healthyNode
    }

    // Send acknowledgment response back to the storage node
    response := &pb.Response{
        Type: pb.RequestType_REPORT_CORRUPTION,
        Response: &pb.Response_ReportCorruptedChunk{
            ReportCorruptedChunk: ackResp,
        },
    }

    err = c.sendResponse(conn, response)
    if err != nil {
        log.Printf("Failed to send response to storage node: %v", err)
    }
}

// selectUniqueNodesForChunk selects 3 random unique nodes for chunk replication
func (c *Controller) selectUniqueNodesForChunk() []string {
	// Get a list of node IDs that have free space
	var availableNodes []string
	for nodeID, node := range c.StorageNodes {
		if node.FreeSpace > 0 {
			availableNodes = append(availableNodes, nodeID)
		}
	}

	// Shuffle the list of available nodes to ensure random selection
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(availableNodes), func(i, j int) {
		availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
	})

	// Select the first 3 nodes after shuffling (or less if there are fewer than 3 nodes)
	replicationFactor := 3
	if len(availableNodes) < replicationFactor {
		replicationFactor = len(availableNodes)
	}

	return availableNodes[:replicationFactor]
}

func (c *Controller) handleAllocateStorage(req *pb.AllocateStorageRequest, conn net.Conn) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Check if the file already exists
	if _, exists := c.FileChunkMap[req.Filename]; exists {
		// Send an error response if the file already exists
		resp := &pb.Response{
			Type: pb.RequestType_ALLOCATE_STORAGE,
			Response: &pb.Response_AllocateStorage{
				AllocateStorage: &pb.AllocateStorageResponse{
					ErrorMessage: "File already exists",
				},
			},
		}
		if err := c.sendResponse(conn, resp); err != nil {
			log.Printf("Failed to send allocate storage error response: %v", err)
		}
		log.Printf("File %s already exists, storage request rejected", req.Filename)
		return
	}

	numChunks := req.ChunkNum
	chunks := make([]*pb.ChunkAllocation, numChunks)

	// For each chunk, select random nodes and create the allocation
	for i := int64(0); i < numChunks; i++ {
		chunkID := fmt.Sprintf("%s_%d", req.Filename, i)

		// Select 3 random nodes for this chunk
		selectedNodes := c.selectUniqueNodesForChunk()

		var nodeInfos []*pb.StorageNodeInfo
		for _, nodeID := range selectedNodes {
			nodeInfo := c.StorageNodes[nodeID]
			nodeInfos = append(nodeInfos, nodeInfo)
			if _, exists := c.NodeChunks[nodeID]; !exists {
				c.NodeChunks[nodeID] = []string{}
			}
			c.NodeChunks[nodeID] = append(c.NodeChunks[nodeID], chunkID)
		}

		// Create chunk allocation with chunk ID and node information
		chunks[i] = &pb.ChunkAllocation{
			ChunkId: chunkID,
			Nodes:   nodeInfos,
		}

		// Map the chunk ID to its file name and chunk number in ChunkFileMap
		c.ChunkFileMap[chunkID] = ChunkFileInfo{
		Filename: req.Filename,
		ChunkNum: i,
	}
	}

	// Store the file-to-chunk mappings
	c.FileChunkMap[req.Filename] = chunks


	// Send the response with allocated chunks back to the client
	resp := &pb.Response{
		Type: pb.RequestType_ALLOCATE_STORAGE,
		Response: &pb.Response_AllocateStorage{
			AllocateStorage: &pb.AllocateStorageResponse{
				ChunkAllocations: chunks,
			},
		},
	}
	if err := c.sendResponse(conn, resp); err != nil {
		log.Printf("Failed to send allocate storage response: %v", err)
	}
}



func (c *Controller) handleGetFileLocations(req *pb.GetFileLocationsRequest, conn net.Conn) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Retrieve the chunk locations for the requested file
	chunkLocations, exists := c.FileChunkMap[req.Filename]
	if !exists {
		log.Printf("File %s not found", req.Filename)
		return
	}

	// Send the response with file locations
	resp := &pb.Response{
		Type: pb.RequestType_GET_FILE_LOCATIONS,
		Response: &pb.Response_GetFileLocations{
			GetFileLocations: &pb.GetFileLocationsResponse{
				ChunkLocations: chunkLocations,
			},
		},
	}
	c.sendResponse(conn, resp)
}

func (c *Controller) handleClientConnection(conn net.Conn) {
	defer conn.Close()

	// Read the incoming message
	req, err := c.readRequest(conn)
	if err != nil {
		log.Printf("Failed to read client request: %v", err)
		return
	}

	switch req.Type {
	case pb.RequestType_ALLOCATE_STORAGE:
		log.Printf("Received allocate storage request")
		c.handleAllocateStorage(req.GetAllocateStorage(), conn)
	case pb.RequestType_GET_FILE_LOCATIONS:
		c.handleGetFileLocations(req.GetGetFileLocations(), conn)
	case pb.RequestType_DELETE_FILE:
		log.Printf("Received delete files request")
		c.handleDeleteFile(req.GetDeleteFile(), conn)
	case pb.RequestType_LIST_FILES:
		log.Printf("Received list files request")
		c.handleListFiles(conn)
	case pb.RequestType_GET_SYSTEM_INFO:
		log.Printf("Received system info request")
		c.handleGetSystemInfo(conn)
	case pb.RequestType_MAP_REDUCE_JOB:
		log.Printf("Received MapReduce job request")
		c.handleMapReduceJob(req.GetMapReduceJob(), conn) // Handle MapReduce job
	default:
		log.Printf("Unknown request type from client")
	}
}

func (c *Controller) handleMapReduceJob(jobReq *pb.MapReduceJobRequest, conn net.Conn) {
	// Validate the MapReduce job request parameters
	if jobReq == nil || jobReq.InputFile == "" || len(jobReq.Plugin) == 0 {
		log.Printf("Invalid MapReduce job request")
		c.sendResponse(conn, &pb.Response{
			Type: pb.RequestType_MAP_REDUCE_JOB,
			Response: &pb.Response_MapReduceJob{
				MapReduceJob: &pb.MapReduceJobResponse{
					JobId:        jobReq.JobId,
					Success:      false,
					ErrorMessage: "Invalid MapReduce job request",
				},
			},
		})
		return
	}

	log.Printf("Starting MapReduce job: %s for file: %s", jobReq.JobId, jobReq.InputFile)

	// Step 1: Locate chunks for the input file
	chunkAllocations, exists := c.FileChunkMap[jobReq.InputFile]
	if !exists {
		log.Printf("No chunks found for input file %s", jobReq.InputFile)
		c.sendResponse(conn, &pb.Response{
			Type: pb.RequestType_MAP_REDUCE_JOB,
			Response: &pb.Response_MapReduceJob{
				MapReduceJob: &pb.MapReduceJobResponse{
					JobId:        jobReq.JobId,
					Success:      false,
					ErrorMessage: "Input file not found",
				},
			},
		})
		return
	}

	c.jobStatus[jobReq.JobId] = make(map[string]bool)
	var wg sync.WaitGroup

	// Step 2: Send plugins and job details to one node per chunk
	for idx, chunk := range chunkAllocations {
		if len(chunk.Nodes) == 0 {
			continue // No available nodes for this chunk, skip it
		}

		// Pick the first available node for this chunk
		selectedNode := c.selectNodeForChunk(chunk)
		chunkID := fmt.Sprintf("%s_%d", jobReq.InputFile, idx)
		c.nodeWorkload[selectedNode.NodeId]++
		c.jobStatus[jobReq.JobId][chunkID] = false // Mark as incomplete

		wg.Add(1)
		go func(node *pb.StorageNodeInfo, jobId, chunkID string) {
			defer wg.Done()

			// Send the job to the storage node
			success := c.sendJobToStorageNode(node, jobId, jobReq.Plugin, chunkID, jobReq.ReducerNum)
				// Update job status and node workload safely
			c.Mutex.Lock()
			defer c.Mutex.Unlock()
			if (success){
				c.jobStatus[jobId][chunkID] = true
				c.nodeWorkload[node.NodeId]-- 
			}
		}(selectedNode, jobReq.JobId, chunkID)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Step 4: Check if all chunks are complete
	allComplete := true
	for _, complete := range c.jobStatus[jobReq.JobId] {
		if !complete {
			allComplete = false
			break
		}
	}

	// Respond to the client based on job completion status
	if allComplete {
		log.Printf("MapReduce job %s completed successfully", jobReq.JobId)
		c.sendResponse(conn, &pb.Response{
			Type: pb.RequestType_MAP_REDUCE_JOB,
			Response: &pb.Response_MapReduceJob{
				MapReduceJob: &pb.MapReduceJobResponse{
					JobId:   jobReq.JobId,
					Success: true,
				},
			},
		})
	} else {
		log.Printf("MapReduce job %s failed", jobReq.JobId)
		c.sendResponse(conn, &pb.Response{
			Type: pb.RequestType_MAP_REDUCE_JOB,
			Response: &pb.Response_MapReduceJob{
				MapReduceJob: &pb.MapReduceJobResponse{
					JobId:        jobReq.JobId,
					Success:      false,
					ErrorMessage: "One or more chunks failed",
				},
			},
		})
	}

	// Clean up job status after completion
	delete(c.jobStatus, jobReq.JobId)
}


func (c *Controller) selectNodeForChunk(chunk *pb.ChunkAllocation) *pb.StorageNodeInfo {
	var selectedNode *pb.StorageNodeInfo
	minWorkload := int(^uint(0) >> 1) // Initialize with max int for comparison

	// Check if there are nodes available in the chunk
	if len(chunk.Nodes) == 0 {
		return nil // No nodes to select from, return nil
	}

	// Iterate over available nodes for this chunk
	for _, node := range chunk.Nodes {
		workload := c.nodeWorkload[node.NodeId]
		if workload < minWorkload {
			selectedNode = node
			minWorkload = workload
		}
	}

	return selectedNode
}


func (c *Controller) sendJobToStorageNode(
	node *pb.StorageNodeInfo,
	jobId string,
	plugin []byte,
	chunkID string,
	reducerNum int32,
) bool {
	// Connect to the storage node
	conn, err := net.Dial("tcp", node.Address)
	if err != nil {
		log.Printf("Failed to connect to storage node %s: %v", node.NodeId, err)
		return false
	}
	defer conn.Close()

	// Create the MapJobRequest with the updated fields
	mapJobRequest := &pb.Request{
		Type: pb.RequestType_MAP_JOB,
		Request: &pb.Request_MapJob{
			MapJob: &pb.MapJobRequest{
				JobId:      jobId,
				ChunkId:    chunkID,
				Plugin:     plugin,        // Combined plugin containing both Map and Reduce functions
				ReducerNum: reducerNum,   // Number of reducers for the job
			},
		},
	}

	// Send the MapJobRequest to the storage node
	if err := c.sendRequest(conn, mapJobRequest); err != nil {
		log.Printf("Failed to send MapJobRequest to node %s: %v", node.NodeId, err)
		return false
	}
	log.Printf("Sent Map job request for job %s to storage node %s", jobId, node.NodeId)

	// Wait for the MapJobResponse from the storage node
	response, err := c.readResponse(conn)
	if err != nil {
		log.Printf("Failed to receive response from node %s: %v", node.NodeId, err)
		return false
	}

	// Process the MapJobResponse
	mapJobResponse := response.GetMapJob()

	// Check the response status and update the job status accordingly
	if mapJobResponse.Success {
		log.Printf("Map job %s for chunk %s completed successfully on node %s", jobId, chunkID, node.NodeId)
		return true
	} else {
		log.Printf("Map job %s for chunk %s failed on node %s: %s", jobId, chunkID, node.NodeId, mapJobResponse.ErrorMessage)
		return false
	}
}



// Handle file deletion request
func (c *Controller) handleDeleteFile(req *pb.DeleteFileRequest, conn net.Conn) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Check if the file exists
	chunks, exists := c.FileChunkMap[req.Filename]
	if !exists {
		// File not found, respond with an error
		resp := &pb.Response{
			Type: pb.RequestType_DELETE_FILE,
			Response: &pb.Response_DeleteFile{
				DeleteFile: &pb.DeleteFileResponse{
					ErrorMessage: "File not found",
				},
			},
		}
		c.sendResponse(conn, resp)
		return
	}

	// Remove the file from FileChunkMap
	delete(c.FileChunkMap, req.Filename)

	// Respond with chunk locations
	resp := &pb.Response{
		Type: pb.RequestType_DELETE_FILE,
		Response: &pb.Response_DeleteFile{
			DeleteFile: &pb.DeleteFileResponse{
				ChunkLocations: chunks,
			},
		},
	}
	c.sendResponse(conn, resp)
}


// Handle List Files Request
func (c *Controller) handleListFiles(conn net.Conn) {
	// Lock the mutex to ensure thread safety when accessing the map
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Gather the list of filenames from the FileChunkMap
	var files []string
	for filename := range c.FileChunkMap {
		files = append(files, filename)
	}

	// Create the ListFilesResponse message
	listFilesResp := &pb.Response{
		Type: pb.RequestType_LIST_FILES,
		Response: &pb.Response_ListFiles{
			ListFiles: &pb.ListFilesResponse{
				Filenames: files,
			},
		},
	}

	// Send the response back to the client
	if err := c.sendResponse(conn, listFilesResp); err != nil {
		log.Printf("Failed to send list files response: %v", err)
	}
}


// Handle Get System Info Request
func (c *Controller) handleGetSystemInfo(conn net.Conn) {
	// Lock the mutex to ensure thread safety when accessing the map
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Gather system information like active nodes, total disk space, etc.
	activeNodes := c.getActiveNodes() // Extract node information from StorageNodes map

	// Create the GetSystemInfoResponse message
	sysInfoResp := &pb.Response{
		Type: pb.RequestType_GET_SYSTEM_INFO,
		Response: &pb.Response_GetSystemInfo{
			GetSystemInfo: &pb.GetSystemInfoResponse{
				ActiveNodes:       activeNodes,
			},
		},
	}

	// Send the response back to the client
	if err := c.sendResponse(conn, sysInfoResp); err != nil {
		log.Printf("Failed to send system info response: %v", err)
	}
}

// Helper method to get active nodes information
func (c *Controller) getActiveNodes() []*pb.StorageNodeInfo {
	var activeNodes []*pb.StorageNodeInfo
	for _, nodeInfo := range c.StorageNodes {
		activeNodes = append(activeNodes, nodeInfo)
	}
	return activeNodes
}

// Send a protobuf request over the connection
func (c *Controller) sendRequest(conn net.Conn, req *pb.Request) error {
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
func (c *Controller) readResponse(conn net.Conn) (*pb.Response, error) {
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

	var resp pb.Response
	if err := proto.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &resp, nil
}


func (c *Controller) readRequest(conn net.Conn) (*pb.Request, error) {
	// Read the length of the message
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	// Read the message
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}

	// Unmarshal the protobuf message
	req := &pb.Request{}
	if err := proto.Unmarshal(data, req); err != nil {
		return nil, err
	}

	return req, nil
}

func (c *Controller) sendResponse(conn net.Conn, resp *pb.Response) error {
	// Marshal the response to protobuf format
	data, err := proto.Marshal(resp)
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

func main() {
	controller := NewController()

	go controller.startStorageNodeServer("5001") // Listen for storage nodes
	go controller.startClientServer("5002")     // Listen for clients
	heartbeatTimeout := 15 * time.Second
	controller.MonitorNodes(heartbeatTimeout)


}
