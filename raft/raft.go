package raft

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

const HeartBeatInterval = 100 * time.Millisecond
const CommitApplyIdleCheckInterval = 100 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex // Lock to protect shared access to this peer's state

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	// General state
	id               string
	me               int // this peer's index into peers[]
	state            ServerState
	isDecommissioned bool

	// Election state
	currentTerm int
	votedFor    string // Id of candidate that has voted for, this term. Empty string if no vote has been cast.
	leaderID    string

	// Log state
	log         []LogEntry
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex     []int     // For each peer, index of next log entry to send that server
	matchIndex    []int     // For each peer, index of highest entry known log entry known to be replicated on peer
	lastEntrySent time.Time // When this node, as Leader, last sent an entry to all nodes

	// Liveness state
	lastHeartBeat time.Time // When this node last received a heartbeat message from the Leader
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// --- RequestVote RPC ---

// RequestVoteArgs - RPC arguments
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
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
		rf.state = Follower
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.VoteGranted = true
	} else if rf.votedFor == "" || args.CandidateID == rf.votedFor { // TODO: Ensure candidates log is at least as up-to-date as our log
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm

	LogInfo("Raft: [Id: %s | Term: %d | %v] - Vote requested for: %s on term: %d. Vote granted? %v", rf.id, rf.currentTerm, rf.state, args.CandidateID, args.Term, reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if voteChan <- server; !ok {
		rf.Lock()
		defer rf.Unlock()
		LogDebug("Raft: [Id: %s | Term: %d | %v] - Communication error: RequestVote() RPC failed", rf.id, rf.currentTerm, rf.state)
	}
}

// --- AppendEntries RPC ---

// AppendEntriesArgs - RPC arguments
type AppendEntriesArgs struct {
	Term             int
	LeaderID         string
	PreviousLogIndex int
	PreviousLogTerm  int
	LogEntries       []LogEntry
	LeaderCommit     int
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
	} else if args.Term >= rf.currentTerm { // Become follower
		rf.state = Follower
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		rf.lastHeartBeat = time.Now()
		rf.votedFor = ""
		reply.Success = true
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.Lock()
		defer rf.Unlock()
		LogDebug("Raft: [Id: %s | Term: %d | %v] - Communication error: AppendEntries() RPC failed", rf.id, rf.currentTerm, rf.state)
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
		peers:       peers,
		persister:   persister,
		me:          me,
		id:          string(rune(me + 'A')),
		state:       Follower,
		commitIndex: -1,
		lastApplied: -1,
	}

	LogInfo("Raft: [Id: %s | Term: %d | %v] - Node created", rf.id, rf.currentTerm, rf.state)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()
	go rf.startCommitProcess(applyCh)

	return rf
}

func (rf *Raft) startCommitProcess(applyChan chan ApplyMsg) {
	rf.Lock()
	LogInfo("Raft: [Id: %s | Term: %d | %v] - Starting commit process - Last log applied: %d", rf.id, rf.currentTerm, rf.state, rf.lastApplied)
	rf.Unlock()

	for {
		rf.Lock()
		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(entries, rf.log[rf.lastApplied+1:rf.commitIndex])
			LogInfo("Raft: [Id: %s | Term: %d | %v] - Locally applying %d log entries: [%d, %d]", rf.id, rf.currentTerm, rf.state, len(entries), rf.lastApplied+1, rf.commitIndex)
			rf.Unlock()

			// Hold no locks on `rf` here so that local applies don't deadlock the system
			for _, v := range entries {
				applyChan <- ApplyMsg{Index: v.Index, Command: v.Command}
			}
		} else {
			rf.Unlock()
			<-time.After(CommitApplyIdleCheckInterval)
		}
	}
}

func (rf *Raft) startElectionTimer() {
	electionTimeout := func() time.Duration { // Randomized timeouts between [500, 600)-ms
		return (500 + time.Duration(rand.Intn(100))) * time.Millisecond
	}

	currentTimeout := electionTimeout()
	currentTime := <-time.After(currentTimeout)

	rf.Lock()
	defer rf.Unlock()
	if !rf.isDecommissioned {
		// Start election process if we're not a leader and the haven't recieved a heartbeat for `electionTimeout`
		if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= currentTimeout {
			LogInfo("Raft: [Id: %s | Term: %d | %v] - Election timer timed out. Timeout: %fs", rf.id, rf.currentTerm, rf.state, currentTimeout.Seconds())
			go rf.startElection()
		}
		go rf.startElectionTimer()
	}
}

func (rf *Raft) startElection() {
	// Increment currentTerm and vote for self
	rf.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id

	LogInfo("Raft: [Id: %s | Term: %d | %v] - Election started", rf.id, rf.currentTerm, rf.state)

	// Request votes from peers
	args, replies := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.id}, make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, voteChan, &args, &replies[i])
		}
	}
	rf.Unlock()

	// Count votes
	votes := 1
	for i := 0; i < len(replies); i++ {
		if replies[<-voteChan].VoteGranted {
			votes++
		}
		if votes > len(replies)/2 { // Has majority vote
			rf.Lock()
			defer rf.Unlock()
			// Ensure that we're still a candidate and that another election did not interrupt
			if rf.state == Candidate && args.Term == rf.currentTerm {
				LogInfo("Raft: [Id: %s | Term: %d | %v] - Election won. Vote: %d/%d", rf.id, rf.currentTerm, rf.state, votes, len(rf.peers))
				go rf.promoteToLeader()
			} else {
				LogInfo("Raft: [Id: %s | Term: %d | %v] - Election for term %d interrupted", rf.id, rf.currentTerm, rf.state, args.Term)
			}
			return
		}
	}

	rf.Lock()
	LogInfo("Raft: [Id: %s | Term: %d | %v] - Election lost. Vote: %d/%d", rf.id, rf.currentTerm, rf.state, votes, len(rf.peers))
	rf.Unlock()
}

func (rf *Raft) promoteToLeader() {
	rf.Lock()
	defer rf.Unlock()

	rf.state = Leader
	rf.leaderID = rf.id

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) // Should be initialized to leader's last log index + 1
		rf.matchIndex[i] = -1         // Index of highest log entry known to be replicated on server
	}

	// Send heartbeats to all peers to inform them that we're the leader
	rf.sendHeartbeats()
	go rf.startLeaderProcess()
}

func (rf *Raft) startLeaderProcess() {
	for {
		currentTime := <-time.After(HeartBeatInterval)

		rf.Lock()
		shouldSendHeartbeats := rf.state == Leader && currentTime.Sub(rf.lastEntrySent) >= HeartBeatInterval
		isDecommissioned := rf.isDecommissioned
		rf.Unlock()

		// If we're leader and haven't had an entry for a while, then send liveness heartbeat
		if _, isLeader := rf.GetState(); !isLeader || isDecommissioned {
			break
		} else if shouldSendHeartbeats {
			rf.Lock()
			rf.sendHeartbeats()
			rf.Unlock()
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	// Heartbeat message
	args, replies := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.id}, make([]AppendEntriesReply, len(rf.peers))

	// Attempt to send heartbeats to all peers
	LogInfo("Raft: [Id: %s | Term: %d | %v] - Sending heartbeats to cluster", rf.id, rf.currentTerm, rf.state)
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &replies[i])
		}
	}

	rf.lastEntrySent = time.Now()
}

// --- Persistence ---

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

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.Lock()
	defer rf.Unlock()

	rf.isDecommissioned = true
	LogInfo("Raft: [Id: %s | Term: %d | %v] - Node killed", rf.id, rf.currentTerm, rf.state)
}
