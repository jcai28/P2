package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"
	"sync"
	"syscall"

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
	TotalRequests  int64
}

func NewStorageNode(nodeID, controllerAddr, storageDir, port string) *StorageNode {
	return &StorageNode{
		NodeID:         nodeID,
		ControllerAddr: controllerAddr,
		StorageDir:     storageDir,
		Port:           port,
	}
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
	default:
		log.Printf("Unknown request type from client")
	}
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



func (s *StorageNode) handleRetrieveChunk(req *dfspb.RetrieveChunkRequest, conn net.Conn) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Construct the file paths for the chunk and checksum
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
		s.sendResponse(conn, resp)
		return
	}

	// Load the chunk from disk
	data, err := ioutil.ReadFile(chunkFilePath)
	if err != nil {
		log.Printf("Failed to retrieve chunk: %v", err)
		// Send error response
		resp := &dfspb.Response{
			Type: dfspb.RequestType_RETRIEVE_CHUNK,
			Response: &dfspb.Response_RetrieveChunk{
				RetrieveChunk: &dfspb.RetrieveChunkResponse{
					ErrorMessage: "Failed to read chunk file",
				},
			},
		}
		s.sendResponse(conn, resp)
		return
	}

	// Load the checksum from the checksum file
	storedChecksum, err := ioutil.ReadFile(checksumFilePath)
	if err != nil {
		log.Printf("Failed to read checksum file: %v", err)
		// Send error response
		resp := &dfspb.Response{
			Type: dfspb.RequestType_RETRIEVE_CHUNK,
			Response: &dfspb.Response_RetrieveChunk{
				RetrieveChunk: &dfspb.RetrieveChunkResponse{
					ErrorMessage: "Failed to read checksum file",
				},
			},
		}
		s.sendResponse(conn, resp)
		return
	}

	// Recalculate the checksum for the loaded chunk data
	calculatedChecksum := s.calculateChecksum(data)

	// Check if the recalculated checksum matches the stored checksum
	if string(storedChecksum) != calculatedChecksum {
		log.Printf("Data corruption detected for chunk %s", req.ChunkId)
		// Handle corruption by sending a recovery request to the controller
		data = s.handleCorruption(req.ChunkId)
		if data == nil {
			log.Println("Data corruption handle failed")
			return
		}
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
				Checksum: string(storedChecksum),
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
