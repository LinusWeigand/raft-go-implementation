package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

// TODO Leader doesn't get majority success responses of heartbeat -> step down
// TODO Node has log and state
// TODO Leader sends new log entries to followers
// TODO Follower append log entries to log
// TODO Leader commits log entries & writes to state
// TODO Leader sends commit message to followers
// TODO Follower commits log entries & writes to state
// TODO LogIndex in Heartbeat
// TODO Error Message if prevLogIndex doesn't match -> send log entries from Leader

const (
	Follower int = iota
	Candidate
	Leader
)

const nodes = 3

type Heartbeat struct {
	Term         int
	LeaderID     string
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	commitIndex  int
}

type HeartbeatResponse struct {
	Term             int
	Success          bool
	LastCorrectIndex int
}

type VoteRequest struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

type Node struct {
	State            int
	HeartbeatTimeout time.Duration
	ElectionTimer    *time.Timer
	CurrentTerm      int
	VotedFor         string
	Id               string
	MajorityReached  chan bool
	StepDown         chan bool
	VotesReceived    map[string]bool
	LastLogIndex     int
	LastLogTerm      int
	CommitIndex      int
	LastCommand      string
	Entries          Entries
}

type Entries []LogEntry

type LogEntry struct {
	Index     int
	Term      int
	Command   string
	Committed bool
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

	//Read flags
	nodeID := flag.String("nodeid", "", "The ID of this node")
	flag.Parse()

	if *nodeID == "" {
		fmt.Println("Node ID is required! \nExample usage: \ngo run main.go --nodeid=node1")
		os.Exit(1)
	} else {
		fmt.Println("Node ID:", *nodeID)
	}

	// Initialize node
	node := &Node{
		State:            Follower,
		HeartbeatTimeout: time.Duration(rand.Intn(200)+100) * time.Millisecond, // 100ms - 300ms
		CurrentTerm:      0,
		VotedFor:         "",
		Id:               *nodeID,
		MajorityReached:  make(chan bool, 1),
		StepDown:         make(chan bool, 1),
		VotesReceived:    make(map[string]bool),
		LastLogIndex:     0,
		LastLogTerm:      0,
		CommitIndex:      0,
		LastCommand:      "",
		Entries:          Entries{},
	}
	node.ElectionTimer = time.AfterFunc(node.HeartbeatTimeout, func() {
		node.becomeCandidate()
	})

	//Read entries from log.txt
	file, err := os.Open("log.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("Failed to close file: %v", err)
		}
	}(file)
	for {
		var index, term int
		var command string
		var committed bool
		_, err := fmt.Fscanf(file, "%d,%d,%s,%t\n", &index, &term, &command, &committed)
		if err != nil {
			break
		}
		node.Entries.Push(LogEntry{index, term, command, committed})
		node.LastLogIndex = index
		node.LastLogTerm = term
		if committed {
			node.CommitIndex = index
		}
		node.LastCommand = command
	}
	go node.listenForStepDown()
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
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}(conn)

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
		n.handleVoteResponse(vr, strings.Split(conn.RemoteAddr().String(), ":")[0])
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
	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.Id
	n.resetElectionTimer()
	n.requestVotes()
	n.VotesReceived[n.Id] = true

	go n.awaitMajority()
}

func (n *Node) awaitMajority() {
	for {
		select {
		case <-n.MajorityReached:
			if n.State == Candidate {
				n.becomeLeader()
				return
			}
		}
	}
}

func (n *Node) listenForStepDown() {
	for {
		select {
		case <-n.StepDown:
			if n.State == Leader || n.State == Candidate {
				n.becomeFollower()
				return
			}
		}
	}
}

func (n *Node) requestVotes() {
	for i := 1; i <= nodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		if nodeID != n.Id {
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
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}(conn)

	// Send type
	err = gob.NewEncoder(conn).Encode("VoteRequest")
	if err != nil {
		log.Printf("Failed to encode message type: %v", err)
		return
	}

	// Send struct
	vr := VoteRequest{
		Term:         n.CurrentTerm,
		CandidateID:  n.Id,
		LastLogIndex: n.LastLogIndex,
		LastLogTerm:  n.LastLogTerm,
	}
	err = gob.NewEncoder(conn).Encode(vr)
	if err != nil {
		log.Printf("Failed to send vote request to %s: %v", nodeID, err)
	}
}

func (n *Node) sendHeartbeats() {
	for i := 1; i <= nodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		if nodeID != n.Id {
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
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}(conn)

	// Send type
	err = gob.NewEncoder(conn).Encode("Heartbeat")
	if err != nil {
		log.Printf("Failed to encode message type: %v", err)
		return
	}

	// Send struct
	hb := Heartbeat{
		Term:         n.CurrentTerm,
		LeaderID:     n.Id,
		prevLogIndex: n.LastLogIndex,
		prevLogTerm:  n.LastLogTerm,
		entries:      n.Entries,
		commitIndex:  n.CommitIndex,
	}
	err = gob.NewEncoder(conn).Encode(hb)
	if err != nil {
		log.Printf("Failed to send heartbeat to %s: %v", nodeID, err)
	}
}

func (n *Node) handleVoteRequest(conn net.Conn, vr VoteRequest) {
	shouldVote := false
	if vr.Term >= n.CurrentTerm {
		if vr.Term > n.CurrentTerm {
			n.CurrentTerm = vr.Term
			n.StepDown <- true
		}
		if (n.VotedFor == "" || n.VotedFor == vr.CandidateID) && vr.LastLogIndex >= n.LastLogIndex && vr.LastLogTerm >= n.LastLogTerm {
			n.VotedFor = vr.CandidateID
			shouldVote = true
		}
	}

	// Send struct
	voteResponse := VoteResponse{
		Term:        n.CurrentTerm,
		VoteGranted: shouldVote,
	}
	err := gob.NewEncoder(conn).Encode(voteResponse)
	if err != nil {
		log.Printf("Error sending vote response: %v", err)
	}

	n.resetElectionTimer()
}

func (n *Node) handleHeartbeat(conn net.Conn, hb Heartbeat) {
	if (n.State == Candidate || n.State == Leader) && hb.Term >= n.CurrentTerm {
		n.State = Follower
		n.VotedFor = ""
		n.StepDown <- true
		return
	}

	//if Leader term is smaller, tell Leader to step down
	if hb.Term < n.CurrentTerm {
		err := gob.NewEncoder(conn).Encode("HeartbeatResponse")
		heartbeatResponse := HeartbeatResponse{
			Term:             n.CurrentTerm,
			Success:          false,
			LastCorrectIndex: n.LastLogIndex,
		}
		err = gob.NewEncoder(conn).Encode(heartbeatResponse)
		if err != nil {
			log.Printf("Error sending heartbeat response: %v", err)
		}
		return
	}

	success := true
	if hb.Term < n.CurrentTerm || hb.prevLogIndex != n.LastLogIndex || hb.prevLogTerm != n.LastLogTerm {
		success = false
	}
	if n.LastCommand != hb.entries[hb.prevLogIndex].Command {
		success = false
		//delete last entry
		n.Entries.Pop()
		n.LastLogIndex--
		n.LastLogTerm = n.Entries[n.LastLogIndex].Term
		n.LastCommand = n.Entries[n.LastLogIndex].Command
	}

	// Send type
	err := gob.NewEncoder(conn).Encode("HeartbeatResponse")
	if err != nil {
		log.Printf("Failed to encode message type: %v", err)
		return
	}
	// Send struct
	heartbeatResponse := HeartbeatResponse{
		Term:    n.CurrentTerm,
		Success: success,
	}
	err = gob.NewEncoder(conn).Encode(heartbeatResponse)
	if err != nil {
		log.Printf("Error sending heartbeat response: %v", err)
	}
	n.resetElectionTimer()
}

func (n *Node) handleVoteResponse(response VoteResponse, voterID string) {
	if !response.VoteGranted {
		n.CurrentTerm = response.Term
		n.State = Follower
		n.VotedFor = ""
		n.StepDown <- true
		return
	}

	if _, ok := n.VotesReceived[voterID]; !ok { // Check if vote is from a new node
		n.VotesReceived[voterID] = true
		if len(n.VotesReceived) >= nodes/2+1 {
			n.MajorityReached <- true
		}
	}
}

func (n *Node) handleHeartbeatResponse(response HeartbeatResponse) {
	if response.Term > n.CurrentTerm {
		n.CurrentTerm = response.Term
		n.State = Follower
		n.VotedFor = ""
		n.StepDown <- true
		return
	}
}

func (n *Node) becomeLeader() {
	n.State = Leader
	n.stopElectionTimer()
	go n.startHeartbeats()
}

func (n *Node) startHeartbeats() {
	ticker := time.NewTicker(n.HeartbeatTimeout)
	for {
		<-ticker.C
		n.sendHeartbeats()
	}
}

func (n *Node) stopElectionTimer() {
	if n.ElectionTimer != nil {
		n.ElectionTimer.Stop()
	}
}

func (n *Node) becomeFollower() {
	n.State = Follower
	n.VotedFor = ""
	n.VotesReceived = make(map[string]bool)
	n.resetElectionTimer()
}

func (n *Node) resetElectionTimer() {
	n.ElectionTimer.Stop()
	n.ElectionTimer.Reset(n.HeartbeatTimeout)
}

func (s *Entries) Push(e LogEntry) {
	*s = append(*s, e)
}

func (s *Entries) Pop() (LogEntry, bool) {
	l := len(*s)
	if l == 0 {
		return LogEntry{}, false
	}
	e := (*s)[l-1]
	*s = (*s)[:l-1]
	return e, true
}

func (s *Entries) Peek() (LogEntry, bool) {
	l := len(*s)
	if l == 0 {
		return LogEntry{}, false
	}
	e := (*s)[l-1]
	return e, true
}

func (s *Entries) IsEmpty() bool {
	return len(*s) == 0
}

func (s *Entries) Size() int {
	return len(*s)
}
