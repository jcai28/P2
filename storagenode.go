package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"bufio"
	"bytes"
	"log"
	"net"
	"os"
	"time"
	"sync"
	"strings"
	"sort"
	"syscall"
	"plugin"

	"google.golang.org/protobuf/proto"
	"dfs/generated/dfspb" // Replace with the actual path
)

type StorageNode struct {
	NodeID         string
	ControllerAddr string
	StorageDir     string
	Port           string
	Conn           net.Conn
	Mutex          sync.Mutex
	pluginMutex    sync.Mutex
	TotalRequests  int64
	pluginCache    map[string]MapReduceFunctions
}

func NewStorageNode(nodeID, controllerAddr, storageDir, port string) *StorageNode {
	return &StorageNode{
		NodeID:         nodeID,
		ControllerAddr: controllerAddr,
		StorageDir:     storageDir,
		Port:           port,
		pluginCache:    make(map[string]MapReduceFunctions),
	}
}

type KeyValue struct {
	Key   string
	Value string
}

type MapReduceFunctions struct {
	mapFunc    func(interface{}, interface{}, func(interface{}, interface{})) error
	reduceFunc func(interface{}, []interface{}, func(interface{}, interface{})) error
}

func (s *StorageNode) sendHeartbeat() {
	for {
		conn, err := net.Dial("tcp", s.ControllerAddr)
		if err != nil {
			log.Printf("Failed to connect to controller: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()

		// Create and send the heartbeat message
		req := &dfspb.Request{
			Type: dfspb.RequestType_HEARTBEAT,
			Request: &dfspb.Request_Heartbeat{
				Heartbeat: &dfspb.HeartbeatRequest{
					NodeInfo: &dfspb.StorageNodeInfo{
						NodeId:       s.NodeID,
						Address:      fmt.Sprintf("%s:%s", getLocalIP(), s.Port),
						FreeSpace:    s.getFreeSpace(), // Update to get actual free space
						TotalRequests: s.TotalRequests,
					},
				},
			},
		}

		data, err := proto.Marshal(req)
		if err != nil {
			log.Printf("Failed to marshal heartbeat request: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		length := uint32(len(data))
		if err := binary.Write(conn, binary.BigEndian, length); err != nil {
			log.Printf("Failed to write request length: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if _, err := conn.Write(data); err != nil {
			log.Printf("Failed to write heartbeat request: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		time.Sleep(5 * time.Second)
	}
}

func getLocalIP() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	addrs, err := net.LookupHost(hostname)
	if err != nil {
		log.Fatalf("Failed to get IP address: %v", err)
	}

	return addrs[0]
}

func (s *StorageNode) handleClientConnection(conn net.Conn) {
	defer conn.Close()

	req, err := s.readRequest(conn)
	if err != nil {
		log.Printf("Failed to read client request: %v", err)
		return
	}

	switch req.Type {
	case dfspb.RequestType_STORE_CHUNK:
		s.handleStoreChunk(req.GetStoreChunk(), conn)
	case dfspb.RequestType_RETRIEVE_CHUNK:
		s.handleRetrieveChunk(req.GetRetrieveChunk(), conn)
	case dfspb.RequestType_DELETE_CHUNK:
		s.handleDeleteChunk(req.GetDeleteChunk(), conn)
	case dfspb.RequestType_ADD_REPLICA: // Handle the add replica request
		s.handleAddReplica(req.GetAddReplica(), conn)
	case dfspb.RequestType_MAP_JOB: // Handle the Map job request
		s.handleMapJob(req.GetMapJob(), conn)
	case dfspb.RequestType_REDUCE_JOB: // Handle the Map job request
		s.handleReduceJob(req.GetReduceJob(), conn)
	default:
		log.Printf("Unknown request type from client")
	}
}

func (s *StorageNode) handleReduceJob(request *dfspb.ReduceJobRequest, conn net.Conn) {
	// Validate the ReduceJobRequest
	if request == nil || request.JobId == "" || len(request.Plugin) == 0 || request.ReducerId == "" {
		log.Printf("Invalid ReduceJobRequest received")
		s.sendResponse(conn, &dfspb.Response{
			Type: dfspb.RequestType_REDUCE_JOB,
			Response: &dfspb.Response_ReduceJob{
				ReduceJob: &dfspb.ReduceJobResponse{
					JobId:        request.JobId,
					ReducerId:    request.ReducerId,
					Success:      false,
					ErrorMessage: "Invalid ReduceJobRequest",
				},
			},
		})
		return
	}

	log.Printf("Processing reduce job: %s on port %d", request.ReducerId, request.Port)

	// Step 1: Initialize the Reduce Function from the Plugin
	_, reduceFunc, err := s.loadPlugin(request.JobId, request.Plugin)
	if err != nil {
		log.Printf("Failed to load reduce plugin for job %s: %v", request.JobId, err)
		s.sendResponse(conn, &dfspb.Response{
			Type: dfspb.RequestType_REDUCE_JOB,
			Response: &dfspb.Response_ReduceJob{
				ReduceJob: &dfspb.ReduceJobResponse{
					JobId:        request.JobId,
					ReducerId:    request.ReducerId,
					Success:      false,
					ErrorMessage: "Failed to load reduce plugin",
				},
			},
		})
		return
	}

	// Step 2: Start Listening for Mapper Files
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", request.Port))
	if err != nil {
		log.Printf("Failed to listen on port %d: %v", request.Port, err)
		s.sendResponse(conn, &dfspb.Response{
			Type: dfspb.RequestType_REDUCE_JOB,
			Response: &dfspb.Response_ReduceJob{
				ReduceJob: &dfspb.ReduceJobResponse{
					JobId:        request.JobId,
					ReducerId:    request.ReducerId,
					Success:      false,
					ErrorMessage: "Failed to listen on port",
				},
			},
		})
		return
	}
	defer listener.Close()

	log.Printf("Listening for mapper files on port %d", request.Port)

	// Create the temp file for appending all mapper outputs
	tempFilePath := fmt.Sprintf("%s/temp_output_%s.txt", s.StorageDir, request.JobId)
	tempFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed to create temp file for mapper outputs: %v", err)
		s.sendResponse(conn, &dfspb.Response{
			Type: dfspb.RequestType_REDUCE_JOB,
			Response: &dfspb.Response_ReduceJob{
				ReduceJob: &dfspb.ReduceJobResponse{
					JobId:        request.JobId,
					ReducerId:    request.ReducerId,
					Success:      false,
					ErrorMessage: "Failed to create temp file",
				},
			},
		})
		return
	}
	defer tempFile.Close()

	// Collect mapper files
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < int(request.MapperNum); i++ {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		wg.Add(1)
		go func(clientConn net.Conn) {
			defer clientConn.Close()
			defer wg.Done()

			// Read message length prefix
			var messageLength int32
			err := binary.Read(clientConn, binary.LittleEndian, &messageLength)
			if err != nil {
				log.Printf("Failed to read message length: %v", err)
				return
			}

			// Read the serialized message
			messageData := make([]byte, messageLength)
			_, err = io.ReadFull(clientConn, messageData)
			if err != nil {
				log.Printf("Failed to read mapper message: %v", err)
				return
			}

			// Unmarshal the message
			var mapperFile dfspb.MapperFile
			err = proto.Unmarshal(messageData, &mapperFile)
			if err != nil {
				log.Printf("Failed to parse mapper message: %v", err)
				return
			}

			// Determine the source of the content
			var content []byte
			if mapperFile.Content == nil {
				// Read content from the specified file
				content, err = os.ReadFile(mapperFile.FileName)
				if err != nil {
					log.Printf("Failed to read file from local path %s: %v", mapperFile.FileName, err)
					return
				}
				log.Printf("Read content from local file: %s", mapperFile.FileName)
			} else {
				// Use the content provided in the message
				content = mapperFile.Content
				log.Printf("Received content for chunk: %s", mapperFile.ChunkId)
			}

			// Append the file content to the temp file
			mu.Lock()
			defer mu.Unlock()
			_, err = tempFile.Write(content)
			if err != nil {
				log.Printf("Failed to append content to temp file: %v", err)
				return
			}

			log.Printf("Appended content from mapper file: %s (chunk: %s)", mapperFile.FileName, mapperFile.ChunkId)
		}(clientConn)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	log.Printf("All mapper files received and appended for job %s", request.JobId)

	// Step 3: Sort and Combine Outputs Using Existing Methods
	outputFilePattern := fmt.Sprintf("%s/sorted_reducer_output_%s_part_%%d.txt", s.StorageDir, request.JobId)

	numReducers := 1 // Adjust if multiple reducers are needed

	// Perform external sorting
	err = s.externalSortFile(tempFilePath, outputFilePattern, numReducers)
	if err != nil {
		log.Printf("Failed to sort mapper outputs for job %s: %v", request.JobId, err)
		s.sendResponse(conn, &dfspb.Response{
			Type: dfspb.RequestType_REDUCE_JOB,
			Response: &dfspb.Response_ReduceJob{
				ReduceJob: &dfspb.ReduceJobResponse{
					JobId:        request.JobId,
					ReducerId:    request.ReducerId,
					Success:      false,
					ErrorMessage: "Failed to sort mapper outputs",
				},
			},
		})
		return
	}

	// Combine sorted outputs and apply the reduce function
	combinedFilePattern := fmt.Sprintf("%s/combined_reducer_output_%s_part_%%d.txt", s.StorageDir, request.JobId)
	err = s.combineSortedOutput(request.JobId, request.ReducerId, outputFilePattern, numReducers, combinedFilePattern, reduceFunc)
	if err != nil {
		log.Printf("Error combining sorted output for job %s: %v", request.JobId, err)
		s.sendResponse(conn, &dfspb.Response{
			Type: dfspb.RequestType_REDUCE_JOB,
			Response: &dfspb.Response_ReduceJob{
				ReduceJob: &dfspb.ReduceJobResponse{
					JobId:        request.JobId,
					ReducerId:    request.ReducerId,
					Success:      false,
					ErrorMessage: fmt.Sprintf("Failed to combine sorted output: %v", err),
				},
			},
		})
		return
	}

	log.Printf("Reduce job %s completed successfully.", request.ReducerId)

	// Step 4: Send Success Response to the Controller
	s.sendResponse(conn, &dfspb.Response{
		Type: dfspb.RequestType_REDUCE_JOB,
		Response: &dfspb.Response_ReduceJob{
			ReduceJob: &dfspb.ReduceJobResponse{
				JobId:        request.JobId,
				ReducerId:    request.ReducerId,
				Success:      true,
				ErrorMessage: "",
			},
		},
	})
}

func (s *StorageNode) handleMapJob(mapJob *dfspb.MapJobRequest, conn net.Conn) {
	log.Printf("Received Map job for job ID: %s on chunk ID: %s", mapJob.JobId, mapJob.ChunkId)

	// Load the plugin functions
	mapFunc, reduceFunc, err := s.loadPlugin(mapJob.JobId, mapJob.Plugin)
	if err != nil {
		errorMessage := fmt.Sprintf("Failed to load plugin: %v", err)
		log.Printf(errorMessage) // Log the error for debugging
		s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, errorMessage)
		return
	}
	
	// Retrieve the chunk data based on chunk ID
	chunkData, _, err := s.getChunkDataWithChecksum(mapJob.ChunkId)
	if err != nil {
		// Log the specific error for debugging purposes
		log.Printf("Failed to retrieve chunk data for chunk ID %s: %v", mapJob.ChunkId, err)
	
		// Include the error message in the response to provide more context
		s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, fmt.Sprintf("Failed to retrieve chunk data: %v", err))
		return
	}
	

	// Create a temporary file for the unsorted output
	tempFile, err := os.CreateTemp(s.StorageDir, fmt.Sprintf("map_output_%s_%s_*.txt", mapJob.JobId, mapJob.ChunkId))
	if err != nil {
		s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, "Failed to create temporary output file")
		return
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name()) // Ensure the temp file is removed after use
	}()

	// Define the emit function to write key-value pairs to the temporary file
	emit := func(key interface{}, value interface{}) {
		_, err := fmt.Fprintf(tempFile, "%v: %v\n", key, value)
		if err != nil {
			log.Printf("Failed to write to temporary output file for job %s, chunk %s: %v", mapJob.JobId, mapJob.ChunkId, err)
		}
	}

	// Process chunk data line by line using bufio.Scanner
	scanner := bufio.NewScanner(bytes.NewReader(chunkData))
	for scanner.Scan() {
		line := scanner.Bytes()
		// Execute the Map function on each line
		if err = mapFunc(mapJob.ChunkId, line, emit); err != nil {
			s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, "Map function execution failed")
			return
		}
	}

	// Check for errors encountered during scanning
	if err := scanner.Err(); err != nil {
		s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, "Failed to read chunk line by line")
		return
	}

	// Close the temp file before sorting it
	tempFile.Close()

	// Define the output file pattern including job ID and chunk ID
	outputFilePattern := fmt.Sprintf("%s/sorted_output_%s_%s_part_%%d.txt", s.StorageDir, mapJob.JobId, mapJob.ChunkId)

	// Sort the temporary file contents and write to multiple sorted output files for reducers
	numReducers := int(mapJob.ReducerNum)
	err = s.externalSortFile(tempFile.Name(), outputFilePattern, numReducers)
	if err != nil {
		s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, "Failed to sort output file")
		return
	}

	combinedFilePattern := fmt.Sprintf("%s/combined_output_%s_%s_part_%%d.txt", s.StorageDir, mapJob.JobId, mapJob.ChunkId)

	// Call combineSortedOutput to group the sorted data by key for each reducer partition
	err = s.combineSortedOutput(mapJob.JobId,mapJob.ChunkId,outputFilePattern, numReducers, combinedFilePattern, reduceFunc)
	if err != nil {
		log.Printf("Error combining sorted output for job %s, chunk %s: %v", mapJob.JobId, mapJob.ChunkId, err)
		s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, fmt.Sprintf("Failed to combine sorted output: %v", err))
		return
	}
	// Send the combined output files to the reducers
	for i, reducer := range mapJob.Reducers {
		reducerFile := fmt.Sprintf(combinedFilePattern, i)
		err := s.sendToReducer(reducerFile, reducer.Address, mapJob.Port, mapJob.ChunkId)
		if err != nil {
			log.Printf("Failed to send file to reducer %s on port %d: %v", reducer.Address, mapJob.Port, err)
			s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, false, fmt.Sprintf("Failed to send to reducer %d: %v", i, err))
			return
		}
	}

	log.Printf("Map function executed, output sorted, combined, and sent to reducers successfully for job ID %s on chunk ID %s", mapJob.JobId, mapJob.ChunkId)

	// Send success response
	s.sendMapJobResponse(conn, mapJob.JobId, mapJob.ChunkId, true, "")
}

// Helper to send output files to reducers
func (s *StorageNode) sendToReducer(filePath, address string, port int32, chunkID string) error {
	// Extract the base address without the port
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address // If no port exists, treat as host
	}

	// Check if the host is the local machine
	localIP := getLocalIP()
	isLocal := (host == localIP )
	newAddress := fmt.Sprintf("%s:%d", host, port)

	// Establish a connection with a timeout
	conn, err := net.DialTimeout("tcp", newAddress, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to reducer at %s: %v", newAddress, err)
	}
	defer conn.Close()

	// Prepare the MapperFile message
	mapperFile := &dfspb.MapperFile{
		FileName: filePath,
		ChunkId:  chunkID,
		Content:  nil, // Content is nil because the file will be accessed locally
	}

	// If the host is not local, include the file content in the message
	if !isLocal {
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", filePath, err)
		}
		defer file.Close()

		// Read the file contents
		content, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %v", filePath, err)
		}
		mapperFile.Content = content
	}

	// Serialize the message to Protobuf
	messageData, err := proto.Marshal(mapperFile)
	if err != nil {
		return fmt.Errorf("failed to marshal MapperFile: %v", err)
	}

	// Send message length first
	messageLength := int32(len(messageData))
	err = binary.Write(conn, binary.LittleEndian, messageLength)
	if err != nil {
		return fmt.Errorf("failed to send message length: %v", err)
	}

	// Send the serialized message
	_, err = conn.Write(messageData)
	if err != nil {
		return fmt.Errorf("failed to send MapperFile to reducer at %s: %v", newAddress, err)
	}

	if isLocal {
		log.Printf("Reducer is local, sent instruction to access file: %s locally", filePath)
	} else {
		log.Printf("Successfully sent MapperFile (size: %d bytes) to reducer at %s", len(messageData), newAddress)
	}
	return nil
}





// Helper function to send the MapJobResponse
func (s *StorageNode) sendMapJobResponse(conn net.Conn, jobId, chunkId string, success bool, errorMessage string) {
	response := &dfspb.Response{
		Type: dfspb.RequestType_MAP_JOB,
		Response: &dfspb.Response_MapJob{
			MapJob: &dfspb.MapJobResponse{
				JobId:        jobId,
				ChunkId:      chunkId,
				Success:      success,
				ErrorMessage: errorMessage,
			},
		},
	}
	if err := s.sendResponse(conn, response); err != nil {
		log.Printf("Failed to send MapJobResponse for job %s: %v", jobId, err)
	}
}

func (s *StorageNode) loadPlugin(jobId string, pluginData []byte) (mapFunc func(interface{}, interface{}, func(interface{}, interface{})) error, reduceFunc func(interface{}, []interface{}, func(interface{}, interface{})) error, err error) {
	// Initialize the plugin cache if it's nil
	if s.pluginCache == nil {
		s.pluginCache = make(map[string]MapReduceFunctions)
	}
	if s.pluginMutex == nil {
		s.pluginMutex = &sync.Mutex{}
	}
	
	s.pluginMutex.Lock()
	defer s.pluginMutex.Unlock()

	// Check if the plugin for this jobId is already loaded in cache
	if cached, exists := s.pluginCache[jobId]; exists {
		log.Printf("%s, Reusing cached map and reduce functions for job %s", s.NodeID, jobId)
		return cached.mapFunc, cached.reduceFunc, nil
	}

	// Generate a unique temporary file name for the job
	tempFile, err := os.CreateTemp("", fmt.Sprintf("plugin_%s_*.so", jobId))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name()) // Ensure the file is cleaned up after loading

	// Write the binary data to the temporary file
	if _, err := tempFile.Write(pluginData); err != nil {
		return nil, nil, fmt.Errorf("failed to write plugin data to file: %v", err)
	}
	tempFile.Close()

	// Load the plugin
	plug, err := plugin.Open(tempFile.Name())
	if err != nil {
		return nil, nil, fmt.Errorf("%s: failed to load plugin: %v", s.NodeID, err)
	}

	// Lookup the Map function
	symMap, err := plug.Lookup("Map")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find Map function in plugin: %v", err)
	}
	mapFunc, ok := symMap.(func(interface{}, interface{}, func(interface{}, interface{})) error)
	if !ok {
		return nil, nil, fmt.Errorf("plugin Map function has unexpected type")
	}

	// Lookup the Reduce function with the correct signature
	symReduce, err := plug.Lookup("Reduce")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find Reduce function in plugin: %v", err)
	}
	reduceFunc, rok := symReduce.(func(interface{}, []interface{}, func(interface{}, interface{})) error)
	if !rok {
		return nil, nil, fmt.Errorf("plugin Reduce function has unexpected type")
	}

	// Cache the map and reduce functions for this jobId
	s.pluginCache[jobId] = MapReduceFunctions{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}

	log.Printf("%s:Map and Reduce functions loaded and cached for job %s", s.NodeID, jobId)
	return mapFunc, reduceFunc, nil
}



func (s *StorageNode) combineSortedOutput(
	jobId string,
	chunkId string,
	inputFilePattern string,
	numReducers int,
	combinedFilePattern string,
	reduceFunc func(interface{}, []interface{}, func(interface{}, interface{})) error,
) error {
	for i := 0; i < numReducers; i++ {
		// Open the sorted output file for each reducer
		inputFileName := fmt.Sprintf(inputFilePattern, i) // e.g., "sorted_output_<job_id>_<chunk_id>_part_%d.txt"
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			return fmt.Errorf("failed to open sorted file %s: %v", inputFileName, err)
		}
		defer inputFile.Close()

		// Generate the combined filename using the passed function
		combinedFileName := fmt.Sprintf(combinedFilePattern, i)

		// Create a new file for the combined output
		combinedFile, err := os.Create(combinedFileName)
		if err != nil {
			return fmt.Errorf("failed to create combined file %s: %v", combinedFileName, err)
		}
		defer combinedFile.Close()
		writer := bufio.NewWriter(combinedFile)

		// Define the emit function to write the final combined output
		emit := func(key interface{}, value interface{}) {
			fmt.Fprintf(writer, "%v: %v\n", key, value)
		}

		// Combine consecutive entries with the same key using the reduce function
		scanner := bufio.NewScanner(inputFile)
		var currentKey string
		var currentValues []interface{}

		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, ": ", 2)
			if len(parts) != 2 {
				continue // Skip malformed lines
			}
			key := parts[0]
			valueStr := parts[1]

			// Convert the value from string to []byte
			valueBytes := []byte(valueStr)

			// If the key changes, apply the reduce function on the previous grouping
			if key != currentKey && currentKey != "" {
				if err := reduceFunc(currentKey, currentValues, emit); err != nil {
					return fmt.Errorf("reduce function failed for key %s: %v", currentKey, err)
				}
				currentValues = []interface{}{}
			}

			// Update the current key and append the []byte value to the list
			currentKey = key
			currentValues = append(currentValues, valueBytes)
		}

		// Apply the reduce function to the last grouping if there's any remaining data
		if len(currentValues) > 0 {
			if err := reduceFunc(currentKey, currentValues, emit); err != nil {
				return fmt.Errorf("reduce function failed for key %s: %v", currentKey, err)
			}
		}

		writer.Flush()
	}

	return nil
}




func (s *StorageNode) externalSortFile(inputFileName, outputFilePattern string, numReducers int) error {
	// Step 1: Split and Sort the input file into sorted chunks
	chunkFiles, err := s.splitAndSortChunks(inputFileName, 100000) // Assuming 100000 pairs per chunk
	if err != nil {
		return fmt.Errorf("failed to split and sort chunks: %v", err)
	}

	// Step 2: Merge the sorted chunk files into multiple output files for reducers
	err = s.mergeSortedChunks(chunkFiles, outputFilePattern, numReducers)
	if err != nil {
		return fmt.Errorf("failed to merge sorted chunks: %v", err)
	}

	// Clean up temporary chunk files with error handling
	for _, chunkFile := range chunkFiles {
		if err := os.Remove(chunkFile); err != nil {
			log.Printf("Failed to delete temporary chunk file %s: %v", chunkFile, err)
		}
	}

	return nil
}




func (s *StorageNode) splitAndSortChunks(inputFileName string, chunkSize int) ([]string, error) {
	file, err := os.Open(inputFileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunkFiles []string
	scanner := bufio.NewScanner(file)
	var kvPairs []KeyValue

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue // Skip malformed lines
		}
		kvPairs = append(kvPairs, KeyValue{Key: parts[0], Value: parts[1]})

		// If we reached the chunk size, sort and save to a temporary file
		if len(kvPairs) >= chunkSize {
			chunkFile, err := s.saveSortedChunk(kvPairs)
			if err != nil {
				return nil, err
			}
			chunkFiles = append(chunkFiles, chunkFile)
			kvPairs = nil // Reset for the next chunk
		}
	}

	// Write any remaining pairs in the last chunk
	if len(kvPairs) > 0 {
		chunkFile, err := s.saveSortedChunk(kvPairs)
		if err != nil {
			return nil, err
		}
		chunkFiles = append(chunkFiles, chunkFile)
	}

	return chunkFiles, nil
}

func (s *StorageNode) saveSortedChunk(kvPairs []KeyValue) (string, error) {
	sort.Slice(kvPairs, func(i, j int) bool {
		return kvPairs[i].Key < kvPairs[j].Key
	})

	tempFile, err := os.CreateTemp(s.StorageDir, "chunk_*.txt")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	for _, kv := range kvPairs {
		fmt.Fprintf(writer, "%s: %s\n", kv.Key, kv.Value)
	}
	writer.Flush()

	return tempFile.Name(), nil
}

func (s *StorageNode) mergeSortedChunks(chunkFiles []string, outputFilePattern string, numReducers int) error {
	// Create multiple output files and writers based on the number of reducers
	writers := make([]*bufio.Writer, numReducers)
	for i := 0; i < numReducers; i++ {
		fileName := fmt.Sprintf(outputFilePattern, i) // e.g., "output_part_%d.txt"
		outputFile, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %v", fileName, err)
		}
		defer outputFile.Close()
		writers[i] = bufio.NewWriter(outputFile)
	}

	// Open each chunk file and create a scanner for each
	scanners := make([]*bufio.Scanner, len(chunkFiles))
	currentPairs := make([]*KeyValue, len(chunkFiles))

	for i, chunkFile := range chunkFiles {
		file, err := os.Open(chunkFile)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		scanners[i] = scanner

		// Initialize currentPairs with the first entry from each chunk
		if scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, ": ", 2)
			if len(parts) == 2 {
				currentPairs[i] = &KeyValue{Key: parts[0], Value: parts[1]}
			}
		}
	}

	// Perform k-way merge without a heap
	for {
		// Find the minimum key among the current pairs
		minIdx := -1
		for i, kv := range currentPairs {
			if kv != nil && (minIdx == -1 || kv.Key < currentPairs[minIdx].Key) {
				minIdx = i
			}
		}

		// If all pairs are nil, we've exhausted all files
		if minIdx == -1 {
			break
		}

		// Determine the reducer partition for the minimum key
		minPair := currentPairs[minIdx]
		reducerIdx := hashKeyToReducer(minPair.Key, numReducers)

		// Write the minimum key-value pair to the appropriate output file
		fmt.Fprintf(writers[reducerIdx], "%s: %s\n", minPair.Key, minPair.Value)

		// Advance the scanner for the file that had the minimum key
		if scanners[minIdx].Scan() {
			line := scanners[minIdx].Text()
			parts := strings.SplitN(line, ": ", 2)
			if len(parts) == 2 {
				currentPairs[minIdx] = &KeyValue{Key: parts[0], Value: parts[1]}
			}
		} else {
			// Mark this file as exhausted
			currentPairs[minIdx] = nil
		}
	}

	// Flush all writers to ensure data is written to files
	for _, writer := range writers {
		writer.Flush()
	}

	return nil
}

// hashKeyToReducer returns the reducer index for a given key
func hashKeyToReducer(key string, numReducers int) int {
	hash := 0
	for _, char := range key {
		hash = (hash*31 + int(char)) % numReducers
	}
	return hash
}




func (s *StorageNode) handleStoreChunk(req *dfspb.StoreChunkRequest, conn net.Conn) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Verify the checksum
	calculatedChecksum := s.calculateChecksum(req.Data)
	if calculatedChecksum != req.Checksum {
		log.Printf("Checksum mismatch for chunk %s: expected %s, got %s", req.ChunkId, req.Checksum, calculatedChecksum)
		resp := &dfspb.Response{
			Type: dfspb.RequestType_STORE_CHUNK,
			Response: &dfspb.Response_StoreChunk{
				StoreChunk: &dfspb.StoreChunkResponse{
					Success:      false,
					ErrorMessage: "Checksum mismatch",
				},
			},
		}
		s.sendResponse(conn, resp)
		return
	}

	// Save the chunk to disk
	filePath := fmt.Sprintf("%s/%s.chunk", s.StorageDir, req.ChunkId)
	if err := ioutil.WriteFile(filePath, req.Data, 0644); err != nil {
		log.Printf("Failed to store chunk: %v", err)
		resp := &dfspb.Response{
			Type: dfspb.RequestType_STORE_CHUNK,
			Response: &dfspb.Response_StoreChunk{
				StoreChunk: &dfspb.StoreChunkResponse{
					Success:      false,
					ErrorMessage: "Failed to store chunk on disk",
				},
			},
		}
		s.sendResponse(conn, resp)
		return
	}

	// Save the checksum to disk
	checksumFilePath := fmt.Sprintf("%s/%s.checksum", s.StorageDir, req.ChunkId)
	if err := ioutil.WriteFile(checksumFilePath, []byte(req.Checksum), 0644); err != nil {
		log.Printf("Failed to store checksum: %v", err)
		// Consider how you want to handle this error; perhaps delete the chunk or notify the client
	}

	// Successfully stored the chunk
	s.TotalRequests++
	log.Printf("Stored chunk %s in %s", req.ChunkId, fmt.Sprintf("%s:%s", getLocalIP(), s.Port))

	// Start the replication process if there are more nodes
	if len(req.Nodes) > 0 {
		nextNode := req.Nodes[0]
		// Launch replication in a separate goroutine for parallelism
		go func() {
			err := s.replicateChunk(req, nextNode.Address)
			if err != nil {
				log.Printf("Failed to replicate chunk %s to node %s: %v", req.ChunkId, nextNode.Address, err)
			} else {
				log.Printf("Replicated chunk %s to node %s", req.ChunkId, nextNode.Address)
			}
		}()
	}

	// Send a success response back to the client
	resp := &dfspb.Response{
		Type: dfspb.RequestType_STORE_CHUNK,
		Response: &dfspb.Response_StoreChunk{
			StoreChunk: &dfspb.StoreChunkResponse{
				Success: true,
			},
		},
	}
	s.sendResponse(conn, resp)
}



func (s *StorageNode) replicateChunk(req *dfspb.StoreChunkRequest, nodeAddress string) error {
	// Connect to the next storage node in the pipeline
	conn, err := net.Dial("tcp", nodeAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to storage node %s: %w", nodeAddress, err)
	}
	defer conn.Close()

	// Prepare the StoreChunkRequest, forwarding to the next node (if any)
	storeReq := &dfspb.Request{
		Type: dfspb.RequestType_STORE_CHUNK,
		Request: &dfspb.Request_StoreChunk{
			StoreChunk: &dfspb.StoreChunkRequest{
				ChunkId:  req.ChunkId,
				Data:     req.Data,
				Checksum: req.Checksum,
				Nodes:    req.Nodes[1:], // Forward to remaining nodes
			},
		},
	}

	// Send the StoreChunkRequest to the next node
	err = s.sendRequest(conn, storeReq)
	if err != nil {
		return fmt.Errorf("failed to send store chunk request: %w", err)
	}

	// Handle the response from the next node
	resp, err := s.readResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to read response from storage node %s: %w", nodeAddress, err)
	}

	if !resp.GetStoreChunk().Success {
		return fmt.Errorf("storage node %s failed to store chunk: %s", nodeAddress, resp.GetStoreChunk().ErrorMessage)
	}

	return nil
}

func (s *StorageNode) handleAddReplica(req *dfspb.AddReplicaRequest, conn net.Conn) error {
    targetNode := req.GetTargetNode()
    
    fmt.Printf("Handling add replica to %s\n", targetNode.Address)
    
    // Initialize nodes slice and append the targetNode
    var nodes []*dfspb.StorageNodeInfo
    
    chunkFilePath := fmt.Sprintf("%s/%s.chunk", s.StorageDir, req.ChunkId)
    checksumFilePath := fmt.Sprintf("%s/%s.checksum", s.StorageDir, req.ChunkId)

    // Check if the chunk file exists
    if _, err := os.Stat(chunkFilePath); os.IsNotExist(err) {
        // File does not exist, send error response
        resp := &dfspb.Response{
            Type: dfspb.RequestType_RETRIEVE_CHUNK,
            Response: &dfspb.Response_RetrieveChunk{
                RetrieveChunk: &dfspb.RetrieveChunkResponse{
                    ErrorMessage: "Chunk not found",
                },
            },
        }
        if sendErr := s.sendResponse(conn, resp); sendErr != nil {
            log.Printf("Failed to send response: %v", sendErr)
            return sendErr
        }
        return nil
    }

    // Load the chunk from disk
    data, err := ioutil.ReadFile(chunkFilePath)
    if err != nil {
        log.Printf("Failed to retrieve chunk: %v", err)
        resp := &dfspb.Response{
            Type: dfspb.RequestType_RETRIEVE_CHUNK,
            Response: &dfspb.Response_RetrieveChunk{
                RetrieveChunk: &dfspb.RetrieveChunkResponse{
                    ErrorMessage: "Failed to read chunk file",
                },
            },
        }
        if sendErr := s.sendResponse(conn, resp); sendErr != nil {
            log.Printf("Failed to send response: %v", sendErr)
            return sendErr
        }
        return err
    }

    // Load the checksum from the checksum file
    storedChecksum, err := ioutil.ReadFile(checksumFilePath)
    if err != nil {
        log.Printf("Failed to read checksum file: %v", err)
        resp := &dfspb.Response{
            Type: dfspb.RequestType_RETRIEVE_CHUNK,
            Response: &dfspb.Response_RetrieveChunk{
                RetrieveChunk: &dfspb.RetrieveChunkResponse{
                    ErrorMessage: "Failed to read checksum file",
                },
            },
        }
        if sendErr := s.sendResponse(conn, resp); sendErr != nil {
            log.Printf("Failed to send response: %v", sendErr)
            return sendErr
        }
        return err
    }

    // Establish a connection to the target node
    targetConn, err := net.Dial("tcp", targetNode.Address)
    if err != nil {
        return fmt.Errorf("failed to connect to target node %s: %w", targetNode.Address, err)
    }
    defer targetConn.Close()

    // Prepare the StoreChunkRequest for the target node
    storeReq := &dfspb.Request{
        Type: dfspb.RequestType_STORE_CHUNK,
        Request: &dfspb.Request_StoreChunk{
            StoreChunk: &dfspb.StoreChunkRequest{
                ChunkId:  req.ChunkId,
                Data:     data,
                Checksum: string(storedChecksum),
                Nodes:    nodes, // Forward to remaining nodes
            },
        },
    }

    // Send the StoreChunkRequest to the target node
    err = s.sendRequest(targetConn, storeReq)
    if err != nil {
        return fmt.Errorf("failed to send store chunk request to target node: %w", err)
    }
    fmt.Printf("StoreChunkRequest sent to %s\n", targetNode.Address)

    // Handle the response from the target node
    resp, err := s.readResponse(targetConn)
    if err != nil {
        log.Printf("Failed to read response from target storage node: %v", err)
        return fmt.Errorf("failed to read response from storage node: %w", err)
    }

    if !resp.GetStoreChunk().Success {
        log.Printf("Storage node %s failed to store chunk: %s", targetNode.Address, resp.GetStoreChunk().ErrorMessage)
        return fmt.Errorf("storage node %s failed to store chunk: %s", targetNode.Address, resp.GetStoreChunk().ErrorMessage)
    }

    // Successfully created the replica
    response := &dfspb.Response{
        Type: dfspb.RequestType_ADD_REPLICA,
        Response: &dfspb.Response_AddReplica{
            AddReplica: &dfspb.AddReplicaResponse{
                Success: true,
            },
        },
    }

    // Send the success response back to the controller
    if sendErr := s.sendResponse(conn, response); sendErr != nil {
        log.Printf("Failed to send add replica response: %v", sendErr)
        return sendErr
    }

    return nil
}

// getChunkDataWithChecksum retrieves the chunk data and checksum for a given chunk ID.
// It verifies the integrity of the data by comparing the calculated and stored checksums.
// If corruption is detected, it can handle recovery if necessary.
func (s *StorageNode) getChunkDataWithChecksum(chunkID string) ([]byte, string, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Construct the file paths for the chunk and checksum
	chunkFilePath := fmt.Sprintf("%s/%s.chunk", s.StorageDir, chunkID)
	checksumFilePath := fmt.Sprintf("%s/%s.checksum", s.StorageDir, chunkID)

	// Check if the chunk file exists
	if _, err := os.Stat(chunkFilePath); os.IsNotExist(err) {
		return nil, "", fmt.Errorf("chunk not found: %s", chunkID)
	}

	// Load the chunk data from disk
	data, err := ioutil.ReadFile(chunkFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read chunk file: %v", err)
	}

	// Load the checksum from the checksum file
	storedChecksum, err := ioutil.ReadFile(checksumFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read checksum file: %v", err)
	}

	// Recalculate the checksum for the loaded chunk data
	calculatedChecksum := s.calculateChecksum(data)

	// Verify if the recalculated checksum matches the stored checksum
	if string(storedChecksum) != calculatedChecksum {
		log.Printf("Data corruption detected for chunk %s", chunkID)
		// Attempt recovery by calling handleCorruption
		data = s.handleCorruption(chunkID)
		if data == nil {
			return nil, "", fmt.Errorf("data corruption could not be handled for chunk: %s", chunkID)
		}
		// Update the checksum after recovery
		calculatedChecksum = s.calculateChecksum(data)
	}
	log.Printf("Data read success for chunk %s", chunkID)

	return data, calculatedChecksum, nil
}



func (s *StorageNode) handleRetrieveChunk(req *dfspb.RetrieveChunkRequest, conn net.Conn) {
	
	// Retrieve the chunk data and checksum
	data, checksum, err := s.getChunkDataWithChecksum(req.ChunkId)
	if err != nil {
		log.Printf("Failed to retrieve chunk %s: %v", req.ChunkId, err)
		// Send error response
		resp := &dfspb.Response{
			Type: dfspb.RequestType_RETRIEVE_CHUNK,
			Response: &dfspb.Response_RetrieveChunk{
				RetrieveChunk: &dfspb.RetrieveChunkResponse{
					ErrorMessage: err.Error(),
				},
			},
		}
		s.sendResponse(conn, resp)
		return
	}


	// Successfully retrieved chunk
	s.TotalRequests++
	log.Printf("Retrieved chunk %s in %s", req.ChunkId, fmt.Sprintf("%s:%s", getLocalIP(), s.Port))

	// Send the chunk data and stored checksum back to the client
	resp := &dfspb.Response{
		Type: dfspb.RequestType_RETRIEVE_CHUNK,
		Response: &dfspb.Response_RetrieveChunk{
			RetrieveChunk: &dfspb.RetrieveChunkResponse{
				Data:     data,
				Checksum: string(checksum),
			},
		},
	}
	s.sendResponse(conn, resp)
}

func (s *StorageNode) handleCorruption(chunkId string) []byte{
    // Step 1: Report the corrupted chunk
    reportReq := &dfspb.Request{
        Type: dfspb.RequestType_REPORT_CORRUPTION,
        Request: &dfspb.Request_ReportCorruptedChunk{
            ReportCorruptedChunk: &dfspb.ReportCorruptedChunkRequest{
                ChunkId: chunkId,
                NodeId:  s.NodeID,
            },
        },
    }

    // Establish a connection to the controller
    conn, err := net.Dial("tcp", s.ControllerAddr)
    if err != nil {
        log.Printf("Failed to connect to controller: %v", err)
        return nil
    }
    defer conn.Close()

    // Send the report request to the controller
    err = s.sendRequest(conn, reportReq)
    if err != nil {
        log.Printf("Failed to send report request for corrupted chunk %s: %v", chunkId, err)
        return nil
    }

    // Receive the controller's response to the report
    reportResp, err := s.readResponse(conn)
    if err != nil {
        log.Printf("Failed to receive response from controller: %v", err)
        return nil
    }
    
    // Check if the report was acknowledged
    if !reportResp.GetReportCorruptedChunk().Acknowledged {
        log.Printf("Controller did not acknowledge the report for chunk %s: %s", chunkId, reportResp.GetReportCorruptedChunk().ErrorMessage)
        return nil
    }

    log.Printf("Controller acknowledged the report for corrupted chunk %s", chunkId)

    // Get the recovery node details
    recoveryNode := reportResp.GetReportCorruptedChunk().HealthyNode
    nodeAddress := recoveryNode.Address
    log.Printf("Controller responded to recovery chunk by calling at %s", nodeAddress)

    // Step 3: Establish a connection to the recovery node
    nodeConn, err := net.Dial("tcp", nodeAddress)
    if err != nil {
        log.Printf("Failed to connect to recovery node at %s: %v", nodeAddress, err)
        return nil
    }
    defer nodeConn.Close()

    // Construct a retrieve chunk request
    retrieveReq := &dfspb.Request{
        Type: dfspb.RequestType_RETRIEVE_CHUNK,
        Request: &dfspb.Request_RetrieveChunk{
            RetrieveChunk: &dfspb.RetrieveChunkRequest{
                ChunkId: chunkId,
            },
        },
    }

    // Send the retrieve chunk request
    err = s.sendRequest(nodeConn, retrieveReq)
    if err != nil {
        log.Printf("Failed to send retrieve request for chunk %s: %v", chunkId, err)
        return nil
    }

    // Receive the retrieve chunk response
    retrieveResp, err := s.readResponse(nodeConn)
    if err != nil {
        log.Printf("Failed to receive response from recovery node: %v", err)
        return nil
    }
	retrieveChunkResp := retrieveResp.GetRetrieveChunk()

    // Save the chunk to disk
    filePath := fmt.Sprintf("%s/%s.chunk", s.StorageDir, chunkId)
    if err := ioutil.WriteFile(filePath, retrieveChunkResp.Data, 0644); err != nil {
        log.Printf("Failed to store chunk: %v", err)
        return nil
    }

    log.Printf("Successfully stored chunk %s on disk", chunkId)
	return retrieveChunkResp.Data

}



// handleDeleteChunk handles chunk deletion requests 
func (s *StorageNode) handleDeleteChunk(req *dfspb.DeleteChunkRequest, conn net.Conn) {
	// Lock the mutex to ensure thread safety
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Get the chunk ID from the request
	chunkID := req.ChunkId

	// Attempt to delete the chunk data from disk
	if err := s.deleteChunkData(chunkID); err != nil {
		// Create a response indicating failure
		resp := &dfspb.Response{
			Type: dfspb.RequestType_DELETE_CHUNK,
			Response: &dfspb.Response_DeleteChunk{
				DeleteChunk: &dfspb.DeleteChunkResponse{
					Success:       false,
					ErrorMessage:  err.Error(),
				},
			},
		}
		s.sendResponse(conn, resp)
		return
	}

	// Create a response indicating success
	resp := &dfspb.Response{
		Type: dfspb.RequestType_DELETE_CHUNK,
		Response: &dfspb.Response_DeleteChunk{
			DeleteChunk: &dfspb.DeleteChunkResponse{
				Success: true,
			},
		},
	}
	s.sendResponse(conn, resp)
}

// deleteChunkData deletes the chunk data and its checksum from disk based on chunkID
func (s *StorageNode) deleteChunkData(chunkID string) error {
	// Construct the path to the chunk file based on chunkID
	chunkFilePath := fmt.Sprintf("%s/%s.chunk", s.StorageDir, chunkID)
	checksumFilePath := fmt.Sprintf("%s/%s.checksum", s.StorageDir, chunkID)

	// Delete the chunk file from disk
	if err := os.Remove(chunkFilePath); err != nil {
		return fmt.Errorf("failed to delete chunk %s: %v", chunkID, err)
	}
	log.Printf("%s is deleted successfully", chunkFilePath)

	// Delete the checksum file from disk
	if err := os.Remove(checksumFilePath); err != nil {
		return fmt.Errorf("failed to delete checksum for chunk %s: %v", chunkID, err)
	}
	log.Printf("%s is deleted successfully", checksumFilePath)

	return nil
}


func (s *StorageNode) readRequest(conn net.Conn) (*dfspb.Request, error) {
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
	req := &dfspb.Request{}
	if err := proto.Unmarshal(data, req); err != nil {
		return nil, err
	}

	return req, nil
}

func (s *StorageNode) sendResponse(conn net.Conn, resp *dfspb.Response) error {
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

// Send a protobuf request over the connection
func (s *StorageNode) sendRequest(conn net.Conn, req *dfspb.Request) error {
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
func (s *StorageNode) readResponse(conn net.Conn) (*dfspb.Response, error) {
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

func (s *StorageNode) getFreeSpace() int64 {
	// Open the storage directory
	dir, err := os.Open(s.StorageDir)
	if err != nil {
		log.Printf("Failed to open storage directory: %v", err)
		return 0
	}
	defer dir.Close()

	// Get filesystem stats for the storage directory
	var stat syscall.Statfs_t
	err = syscall.Statfs(s.StorageDir, &stat)
	if err != nil {
		log.Printf("Failed to get filesystem stats for storage directory: %v", err)
		return 0
	}

	// Calculate the available free space in bytes
	freeSpace := int64(stat.Bavail) * int64(stat.Bsize)/ 1024/1024

	//log.Printf("Free space in storage directory: %d bytes", freeSpace)
	return freeSpace
}


// Calculate checksum for a chunk of data
func (s *StorageNode) calculateChecksum(data []byte) string {
	hash := sha256.New()
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <node_id> <storage_dir> <port>", os.Args[0])
	}

	nodeID := os.Args[1]
	controllerAddr := "orion01:5001"
	storageDir := os.Args[2]
	port := os.Args[3]

	storageNode := NewStorageNode(nodeID, controllerAddr, storageDir, port)

	// Start listening for client connections
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start client server: %v", err)
	}
	defer listener.Close()

	go storageNode.sendHeartbeat() // Start sending heartbeats

	log.Printf("Storage node listening for clients on port %s...", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept client connection: %v", err)
			continue
		}
		go storageNode.handleClientConnection(conn) // Handle client connections
	}
}
