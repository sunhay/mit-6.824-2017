package raft

import (
	"math/rand"
	"sync"
	"time"

	"fmt"
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
const LeaderPeerTickInterval = 10 * time.Millisecond

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
	nextIndex      []int // For each peer, index of next log entry to send that server
	matchIndex     []int // For each peer, index of highest entry known log entry known to be replicated on peer
	sendAppendChan []chan struct{}

	// Liveness state
	lastHeartBeat time.Time // When this node last received a heartbeat message from the Leader
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("LogEntry(Index: %d, Term: %d, Command: %d)", entry.Index, entry.Term, entry.Command)
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

	lastIndex, lastTerm := func() (int, int) {
		if len(rf.log) > 0 {
			entry := rf.log[len(rf.log)-1]
			return entry.Index, entry.Term
		}
		return 0, 0
	}()

	logIsUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}
		return lastTerm < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm && logIsUpToDate {
		rf.state = Follower
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.VoteGranted = true
	} else if (rf.votedFor == "" || args.CandidateID == rf.votedFor) && logIsUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

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
	reply.Term = rf.currentTerm

	LogDebug("Raft: [Id: %s | Term: %d | %v] - Request from %s, w/ %d entries. Prev:[Index %d, Term %d], Log:%s", rf.id, rf.currentTerm, rf.state, args.LeaderID, len(args.LogEntries), args.PreviousLogIndex, args.PreviousLogTerm, rf.log)

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.state = Follower
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		rf.votedFor = ""
	}

	if rf.leaderID == args.LeaderID {
		rf.lastHeartBeat = time.Now()
	}

	prevLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PreviousLogIndex && v.Term == args.PreviousLogTerm {
			prevLogIndex = i
			break
		}
	}

	logsAreEmpty := args.PreviousLogIndex == 0 && len(rf.log) == 0

	if prevLogIndex >= 0 || logsAreEmpty {
		if len(args.LogEntries) > 0 {
			LogInfo("Raft: [Id: %s | Term: %d | %v] - Appending %d entries from %s", rf.id, rf.currentTerm, rf.state, len(args.LogEntries), args.LeaderID)
		}

		// Update existing log entries
		entriesIndex := 0
		for i := prevLogIndex + 1; i < len(rf.log); i++ {
			// TODO: Confirm this logic is right
			if entriesIndex < len(args.LogEntries) && rf.log[i].Index == args.LogEntries[entriesIndex].Index {
				if rf.log[i].Term != rf.log[i].Term {
					rf.log = rf.log[:i] // Delete all existing entries as they are inconsistent
					break
				} else {
					entriesIndex++
				}
			} else {
				LogInfo("Raft: [Id: %s | Term: %d | %v] - AppendEntries entry index match failure. That isn't good!", rf.id, rf.currentTerm, rf.state)
				return
			}
		}

		// Append new entries that are not already in log
		if entriesIndex < len(args.LogEntries) {
			rf.log = append(rf.log, args.LogEntries[entriesIndex:]...)
		}

		// Update commit index
		if args.LeaderCommit > rf.commitIndex {
			latestLogIndex := rf.log[len(rf.log)-1].Index
			if args.LeaderCommit < latestLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = latestLogIndex
			}
		}
		reply.Success = true
	} else {
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	if rf.state != Leader || rf.isDecommissioned {
		rf.Unlock()
		return
	}

	var entries []LogEntry = []LogEntry{}
	var prevLogIndex, prevLogTerm int = 0, 0

	peerId := string(rune(peerIndex + 'A'))
	lastLogIndex := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index
		}
		return 0
	}()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {
		// Need to send logs beginning from index `rf.nextIndex[peerIndex]`
		for i, v := range rf.log {
			if v.Index == rf.nextIndex[peerIndex] {
				if i > 0 {
					lastEntry := rf.log[i-1]
					prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
				}
				entries = make([]LogEntry, len(rf.log)-i)
				copy(entries, rf.log[i:])
				break
			}
		}
		LogDebug("Raft: [Id: %s | Term: %d | %v] - Sending log %d entries to %s", rf.id, rf.currentTerm, rf.state, len(entries), peerId)
	} else { // We're just going to send a heartbeat
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		}
	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:             rf.currentTerm,
		LeaderID:         rf.id,
		PreviousLogIndex: prevLogIndex,
		PreviousLogTerm:  prevLogTerm,
		LogEntries:       entries,
		LeaderCommit:     rf.commitIndex,
	}
	rf.Unlock()

	ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply)

	rf.Lock()
	defer rf.Unlock()

	if !ok {
		LogDebug("Raft: [Id: %s | Term: %d | %v] - Communication error: AppendEntries() RPC failed", rf.id, rf.currentTerm, rf.state)
		return
	}

	if reply.Success {
		if len(entries) > 0 {
			LogInfo("Raft: [Id: %s | Term: %d | %v] - Appended %d entries to log of %s. Log size: %d entries", rf.id, rf.currentTerm, rf.state, len(entries), peerId, len(rf.log))
			lastReplicated := entries[len(entries)-1]
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.updateCommitIndex()
		} else {
			LogDebug("Raft: [Id: %s | Term: %d | %v] - Successful heartbeat from %s", rf.id, rf.currentTerm, rf.state, peerId)
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = ""
		} else {
			LogInfo("Raft: [Id: %s | Term: %d | %v] - Deviation on peer: %s, term: %d, nextIndex: %d", rf.id, rf.currentTerm, rf.state, peerId, reply.Term, rf.nextIndex[peerIndex])
			rf.nextIndex[peerIndex]--    // Log deviation, we should go back an entry and see if we can correct it
			sendAppendChan <- struct{}{} // Signal to leader-peer process that appends need to occur
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for _, v := range rf.log {
		// We can only increment commit index if our larger entry is for the current term
		if v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			// Check to see if majority of nodes have replicated this
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me {
					if rf.matchIndex[j] >= v.Index {
						if replicationCount++; replicationCount > len(rf.peers)/2 {
							LogInfo("Raft: [Id: %s | Term: %d | %v] - Updating commit index: [%d -> %d], Replication: %d/%d", rf.id, rf.currentTerm, rf.state, rf.commitIndex, v.Index, replicationCount, len(rf.peers))
							// Set index of this entry as new commit index
							rf.commitIndex = v.Index
						}
					}
				}
			}
		}
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
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}

	rf.Lock()
	defer rf.Unlock()

	nextIndex := func() int {
		if len(rf.log) > 0 {
			return len(rf.log) + 1
		}
		return 1
	}()

	rf.log = append(rf.log, LogEntry{Index: nextIndex, Term: rf.currentTerm, Command: command})

	LogInfo("Raft: [Id: %s | Term: %d | %v] - New entry appended to leader's log: %s", rf.id, rf.currentTerm, rf.state, rf.log[nextIndex-1])

	return nextIndex, term, isLeader
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
		commitIndex: 0,
		lastApplied: 0,
	}

	LogInfo("Raft: [Id: %s | Term: %d | %v] - Node created", rf.id, rf.currentTerm, rf.state)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()
	go rf.startLocalApplyProcess(applyCh)

	return rf
}

func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	rf.Lock()
	LogInfo("Raft: [Id: %s | Term: %d | %v] - Starting commit process - Last log applied: %d", rf.id, rf.currentTerm, rf.state, rf.lastApplied)
	rf.Unlock()

	for {
		rf.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(entries, rf.log[rf.lastApplied:rf.commitIndex])
			LogInfo("Raft: [Id: %s | Term: %d | %v] - Locally applying %d log entries. lastApplied: %d. commitIndex: %d", rf.id, rf.currentTerm, rf.state, len(entries), rf.lastApplied, rf.commitIndex)
			rf.Unlock()

			for _, v := range entries { // Hold no locks so that slow local applies don't deadlock the system
				LogInfo("Raft: [Id: %s | Term: %d | %v] - Locally applying log: %s", rf.id, rf.currentTerm, rf.state, v)
				applyChan <- ApplyMsg{Index: v.Index, Command: v.Command}
			}

			rf.Lock()
			rf.lastApplied += len(entries)
			rf.Unlock()
		} else {
			rf.Unlock()
			<-time.After(CommitApplyIdleCheckInterval)
		}
	}
}

func (rf *Raft) startElectionProcess() {
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
			go rf.beginElection()
		}
		go rf.startElectionProcess()
	}
}

func (rf *Raft) beginElection() {
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
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1 // Should be initialized to leader's last log index + 1
			rf.matchIndex[i] = 0              // Index of hiqhest log entry known to be replicated on server
			rf.sendAppendChan[i] = make(chan struct{}, 1)

			// Start routines for each peer which will be used to monitor and send log entries
			go rf.startLeaderPeerProcess(i, rf.sendAppendChan[i])
		}
	}
}

func (rf *Raft) startLeaderPeerProcess(peerIndex int, sendAppendChan chan struct{}) {
	ticker := time.NewTicker(LeaderPeerTickInterval)

	// Initial heartbeat
	rf.sendAppendEntries(peerIndex, sendAppendChan)
	lastEntrySent := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader || rf.isDecommissioned {
			ticker.Stop()
			rf.Unlock()
			break
		}
		rf.Unlock()

		select {
		case <-sendAppendChan: // Signal that we should send a new append to this peer
			lastEntrySent = time.Now()
			rf.sendAppendEntries(peerIndex, sendAppendChan)
		case currentTime := <-ticker.C: // If traffic has been idle, we should send a heartbeat
			if currentTime.Sub(lastEntrySent) >= HeartBeatInterval {
				lastEntrySent = time.Now()
				rf.sendAppendEntries(peerIndex, sendAppendChan)
			}
		}
	}
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
