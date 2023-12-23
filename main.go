package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

const (
	Follower int = iota
	Candidate
	Leader
)

const nodes = 3

type Heartbeat struct {
	Term    int
	Message string
}

type HeartbeatResponse struct {
	Term    int
	Success bool
}

type VoteRequest struct {
	Term        int
	CandidateID string
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

type Node struct {
	state            int
	heartbeatTimeout time.Duration
	electionTimer    *time.Timer
	currentTerm      int
	votedFor         string
	id               string
	majorityReached  chan bool
	voteCount        int
	stepDown         chan bool
}

func main() {
	/*
	 Some notes for my team members:
	 We use a channel for bool to have a signal where we can react on if the values changes.
	 The go keyword before a function call makes the function run in a new thread.
	 The communication between nodes goes as follows:
	 For every Message, Response Pair we establish a tcp connection.
	 First we send the message type (VoteRequest, VoteRequestResponse, Heartbeat, HeartbeatResponse), then the type's struct.
	*/

	nodeID := flag.String("nodeid", "", "The ID of this node")
	flag.Parse()

	if *nodeID == "" {
		fmt.Println("Node ID is required! \nExample usage: \ngo run main.go --nodeid=node1")
		os.Exit(1)
	} else {
		fmt.Println("Node ID:", *nodeID)
	}

	node := &Node{
		state:            Follower,
		heartbeatTimeout: time.Duration(rand.Intn(200)+100) * time.Millisecond, // 100ms - 300ms
		currentTerm:      0,
		votedFor:         "",
		id:               *nodeID,
		majorityReached:  make(chan bool, 1),
		stepDown:         make(chan bool, 1),
		voteCount:        0,
	}
	node.electionTimer = time.AfterFunc(node.heartbeatTimeout, func() {
		node.becomeCandidate()
	})

	go node.startServer()
}

func (n *Node) startServer() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer func(ln net.Listener) {
		err := ln.Close()
		if err != nil {
			log.Printf("Failed to close listener: %v", err)
		}
	}(ln)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	var msgType string
	err := gob.NewDecoder(conn).Decode(&msgType)
	if err != nil {
		log.Printf("Error decoding message type: %v", err)
		return
	}

	switch msgType {
	case "VoteRequest":
		var vr VoteRequest
		err := gob.NewDecoder(conn).Decode(&vr)
		if err != nil {
			log.Printf("Error decoding vote request: %v", err)
			return
		}
		n.handleVoteRequest(conn, vr)
	case "Heartbeat":
		var hb Heartbeat
		err := gob.NewDecoder(conn).Decode(&hb)
		if err != nil {
			log.Printf("Error decoding heartbeat: %v", err)
			return
		}
		n.handleHeartbeat(conn, hb)
	case "VoteResponse":
		var vr VoteResponse
		err := gob.NewDecoder(conn).Decode(&vr)
		if err != nil {
			log.Printf("Error decoding vote response: %v", err)
			return
		}
		n.handleVoteResponse(vr)
	case "HeartbeatResponse":
		var hr HeartbeatResponse
		err := gob.NewDecoder(conn).Decode(&hr)
		if err != nil {
			log.Printf("Error decoding heartbeat response: %v", err)
			return
		}
		n.handleHeartbeatResponse(hr)
	}
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.voteCount = 1
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimer()
	n.requestVotes()

	go n.awaitMajority()
}

func (n *Node) awaitMajority() {
	for {
		select {
		case <-n.majorityReached:
			if n.state == Candidate {
				n.becomeLeader()
				return
			}
		case <-n.stepDown:
			if n.state == Candidate {
				n.state = Follower
				return
			}
		case <-time.After(n.heartbeatTimeout):
			n.becomeCandidate()
			return
		}
	}
}

func (n *Node) resetElectionTimer() {
	n.electionTimer.Stop()
	n.electionTimer.Reset(n.heartbeatTimeout)
}

func (n *Node) requestVotes() {
	for i := 1; i <= nodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		if nodeID != n.id {
			go n.sendVoteRequest(nodeID)
		}
	}
}

func (n *Node) sendVoteRequest(nodeID string) {
	// Establish TCP connection
	conn, err := net.Dial("tcp", nodeID+":8080")
	if err != nil {
		log.Printf("Failed to connect to %s: %v", nodeID, err)
		return
	}
	defer conn.Close()

	// Send type
	err = gob.NewEncoder(conn).Encode("VoteRequest")
	if err != nil {
		log.Printf("Failed to encode message type: %v", err)
		return
	}

	// Send struct
	vr := VoteRequest{
		Term:        n.currentTerm,
		CandidateID: n.id,
	}
	err = gob.NewEncoder(conn).Encode(vr)
	if err != nil {
		log.Printf("Failed to send vote request to %s: %v", nodeID, err)
	}
}

func (n *Node) sendHeartbeats() {
	for i := 1; i <= nodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		if nodeID != n.id {
			go n.sendHeartbeatMessage(nodeID)
		}
	}
}

func (n *Node) sendHeartbeatMessage(nodeID string) {
	// Establish TCP connection
	conn, err := net.Dial("tcp", nodeID+":8080")
	if err != nil {
		log.Printf("Failed to connect to %s: %v", nodeID, err)
		return
	}
	defer conn.Close()

	// Send type
	err = gob.NewEncoder(conn).Encode("Heartbeat")
	if err != nil {
		log.Printf("Failed to encode message type: %v", err)
		return
	}

	// Send struct
	hb := Heartbeat{
		Term:    n.currentTerm,
		Message: "Heartbeat",
	}
	err = gob.NewEncoder(conn).Encode(hb)
	if err != nil {
		log.Printf("Failed to send heartbeat to %s: %v", nodeID, err)
	}
}

func (n *Node) handleVoteRequest(conn net.Conn, vr VoteRequest) {
	shouldVote := false
	if vr.Term >= n.currentTerm {
		if vr.Term > n.currentTerm {
			n.currentTerm = vr.Term
			n.state = Follower
			n.votedFor = ""
		}
		if n.votedFor == "" || n.votedFor == vr.CandidateID {
			n.votedFor = vr.CandidateID
			shouldVote = true
		}
	}

	// Send struct
	voteResponse := VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: shouldVote,
	}
	err := gob.NewEncoder(conn).Encode(voteResponse)
	if err != nil {
		log.Printf("Error sending vote response: %v", err)
	}

	n.resetElectionTimer()
}

func (n *Node) handleHeartbeat(conn net.Conn, hb Heartbeat) {
	success := false
	if hb.Term >= n.currentTerm {
		n.currentTerm = hb.Term
		n.state = Follower
		n.votedFor = ""
		success = true
	}

	// Send struct
	heartbeatResponse := HeartbeatResponse{
		Term:    n.currentTerm,
		Success: success,
	}

	err := gob.NewEncoder(conn).Encode(heartbeatResponse)
	if err != nil {
		log.Printf("Error sending heartbeat response: %v", err)
	}
	n.resetElectionTimer()
}

func (n *Node) handleVoteResponse(response VoteResponse) {
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.state = Follower
		n.votedFor = ""
		n.stepDown <- true
		return
	}

	if response.VoteGranted {
		n.voteCount++
		if n.voteCount > nodes/2 {
			n.majorityReached <- true
		}
	}
}

func (n *Node) handleHeartbeatResponse(response HeartbeatResponse) {
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.state = Follower
		n.votedFor = ""
		n.stepDown <- true
		return
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	go n.startHeartbeats()
}

func (n *Node) startHeartbeats() {
	ticker := time.NewTicker(n.heartbeatTimeout)
	for {
		<-ticker.C
		n.sendHeartbeats()
	}
}
