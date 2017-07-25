package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"

	"github.com/sunhay/scratchpad/golang/mit-6.824-2017/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type ServerState string

const (
	Follower  ServerState = "Follower"
	Candidate             = "Candidate"
	Leader                = "Leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex // Lock to protect shared access to this peer's state

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	id              string
	me              int // this peer's index into peers[]
	state           ServerState
	isDecommisioned bool

	currentTerm int
	votedFor    string // Id of candidate that has voted for, this term. Empty string if no vote has been cast.
	leaderID    string

	lastHeartBeat time.Time
	lastEntrySent time.Time

	// TODO: Add log storage state
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// --- RequestVote RPC ---

// RequestVoteArgs - RPC arguments
type RequestVoteArgs struct {
	Term        int
	CandidateID string
	// TODO: Last log index/term
}

// RequestVoteReply - RPC response
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote - RPC function
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.CandidateID
	} else if rf.votedFor == "" || args.CandidateID == rf.votedFor {
		// TODO: Ensure candidates log is at least as up-to-date as our log
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	reply.Term = rf.currentTerm

	DPrintf("Raft: [Id: %s | Term: %d | %v] - Vote requested for: %s on term: %d. Vote granted? %v", rf.id, rf.currentTerm, rf.state, args.CandidateID, args.Term, reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(server int, wg *sync.WaitGroup, args *RequestVoteArgs, reply *RequestVoteReply) {
	defer wg.Done()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("Raft: [Id: %s | Term: %d | %v] - Communication error: RequestVote() RPC failed", rf.id, rf.currentTerm, rf.state)
	}
}

// --- AppendEntries RPC ---

// AppendEntriesArgs - RPC arguments
type AppendEntriesArgs struct {
	Term     int
	LeaderID string
}

// AppendEntriesReply - RPC response
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries - RPC function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm { // Become follower
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		rf.state = Follower
		reply.Success = true
		rf.votedFor = ""
		rf.lastHeartBeat = time.Now()
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("Raft: [Id: %s | Term: %d | %v] - Communication error: AppendEntries() RPC failed", rf.id, rf.currentTerm, rf.state)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		id:        string(rune(me + 'A')),
		state:     Follower,
	}

	DPrintf("Raft: [Id: %s | Term: %d | %v] - Node created", rf.id, rf.currentTerm, rf.state)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimer()

	return rf
}

func electionTimeout() time.Duration {
	return (600 + time.Duration(rand.Intn(100))) * time.Millisecond
}

func (rf *Raft) electionTimer() {
	timeout := electionTimeout()
	currentTime := <-time.After(timeout)

	rf.Lock()
	defer rf.Unlock()
	// Start election process if we're not a leader and the haven't recieved a heartbeat for `electionTimeout`
	if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= timeout {
		DPrintf("Raft: [Id: %s | Term: %d | %v] - Election timer timed out. Timeout: %fs", rf.id, rf.currentTerm, rf.state, timeout.Seconds())
		go rf.startElection()
	} else if !rf.isDecommisioned {
		go rf.electionTimer()
	}
}

func (rf *Raft) startElection() {
	// Increment currentTerm and vote for self
	rf.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id

	DPrintf("Raft: [Id: %s | Term: %d | %v] - Election started", rf.id, rf.currentTerm, rf.state)

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.id}
	resps := make([]RequestVoteReply, len(rf.peers))
	rf.Unlock()

	// Reset election timer in case of split vote / general unavailability
	go rf.electionTimer()

	// Request votes from peers
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go rf.sendRequestVote(i, &wg, &args, &resps[i])
		}
	}
	wg.Wait()

	rf.Lock()
	defer rf.Unlock()

	// Ensure that we're still a candidate and that another election did not start
	if rf.state == Candidate && args.Term == rf.currentTerm {
		votes := 1 // Count up votes (including self)
		for i := range rf.peers {
			if i != rf.me && resps[i].VoteGranted {
				votes++
			}
		}

		DPrintf("Raft: [Id: %s | Term: %d | %v] - Election results. Vote: %d/%d", rf.id, rf.currentTerm, rf.state, votes, len(rf.peers))

		// If majority vote, become leader
		if votes > len(rf.peers)/2 {
			rf.state = Leader
			rf.leaderID = rf.id
			go rf.heartbeatTimer()
		}
	} else {
		DPrintf("Raft: [Id: %s | Term: %d | %v] - Election for term %d interrupted", rf.id, rf.currentTerm, rf.state, args.Term)
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.Lock()

	// Heartbeat message
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.id}
	resps := make([]AppendEntriesReply, len(rf.peers))

	DPrintf("Raft: [Id: %s | Term: %d | %v] - Sending heartbeats to cluster", rf.id, rf.currentTerm, rf.state)

	// Attempt to send heartbeats to all peers
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &resps[i])
		}
	}
	rf.lastEntrySent = time.Now()

	rf.Unlock()
}

func (rf *Raft) heartbeatTimer() {
	rf.sendHeartbeats() // Send heartbeats to all peers as we've just become leader
	for {
		timeout := 300 * time.Millisecond
		currentTime := <-time.After(timeout)

		rf.Lock()
		shouldSendHeartbeats := rf.state == Leader && currentTime.Sub(rf.lastEntrySent) >= timeout
		isDecomissioned := rf.isDecommisioned
		rf.Unlock()

		// If we're leader and haven't had an entry for a while, then send liveness heartbeat
		if _, isLeader := rf.GetState(); !isLeader || isDecomissioned {
			break
		} else if shouldSendHeartbeats {
			rf.sendHeartbeats()
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.Lock()
	defer rf.Unlock()
	rf.isDecommisioned = true
	DPrintf("Raft: [Id: %s | Term: %d | %v] - Node killed", rf.id, rf.currentTerm, rf.state)
}
