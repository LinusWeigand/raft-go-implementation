package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
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
	// Some notes for my team members:
	// We use a channel for bool to have a signal where we can react on if the values changes
	// The communication between nodes goes as follows:
	// For every message we establish a tcp connection.
	// First we send the message type (VoteRequest, VoteRequestResponse, Heartbeat, HeartbeatResponse), then the type's struct.

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
	}
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.voteCount = 1
	n.majorityReached = make(chan bool, 1)
	n.stepDown = make(chan bool, 1)
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
				// Step down from candidacy and become a follower
				n.state = Follower
				return
			}
		case <-time.After(n.heartbeatTimeout):
			// Election timeout; start a new election
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

func (n *Node) handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	// Parse the term and candidate ID from the request
	candidateTerm, err := strconv.Atoi(r.URL.Query().Get("term"))
	if err != nil {
		http.Error(w, "Invalid term", http.StatusBadRequest)
		return
	}
	candidateID := r.URL.Query().Get("candidateId")

	shouldVote := false

	// Compare the candidate's term to the node's current term
	if candidateTerm >= n.currentTerm {
		// Update the node's term if the candidate's term is higher
		if candidateTerm > n.currentTerm {
			n.currentTerm = candidateTerm
			n.state = Follower
			n.votedFor = ""
		}

		// Vote for the candidate if the node hasn't voted for anyone else in this term
		if n.votedFor == "" || n.votedFor == candidateID {
			n.votedFor = candidateID
			shouldVote = true
		}
	}

	// Send the vote response
	if shouldVote {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Voted for %s", candidateID)
	} else {
		http.Error(w, "Vote not granted", http.StatusForbidden)
	}
	n.resetElectionTimer()
}

func (n *Node) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// Example: Extract query parameters (like leaderTerm)
	// leaderTerm := r.URL.Query().Get("leaderTerm")

	// Implement the logic for handling heartbeat here
	// ...

	// Reset the election timer on receiving a heartbeat
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	// Perform any additional actions needed for transitioning to leader
	// e.g., start sending heartbeats to followers
	go n.startHeartbeats()
}

func (n *Node) startHeartbeats() {
	ticker := time.NewTicker(n.heartbeatTimeout)
	for {
		<-ticker.C
		n.sendHeartbeats()
	}
}
