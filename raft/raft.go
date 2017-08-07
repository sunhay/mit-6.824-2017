package raft

import (
	"math/rand"
	"sync"
	"time"

	"bytes"
	"encoding/gob"

	"github.com/sunhay/mit-6.824-2017/labrpc"
)

type ServerState string

const (
	Follower  ServerState = "Follower"
	Candidate             = "Candidate"
	Leader                = "Leader"
)

const HeartBeatInterval = 100 * time.Millisecond
const CommitApplyIdleCheckInterval = 25 * time.Millisecond
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

	// Log compaction state, if snapshots are enabled
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// Leader state
	nextIndex      []int // For each peer, index of next log entry to send that server
	matchIndex     []int // For each peer, index of highest entry known log entry known to be replicated on peer
	sendAppendChan []chan struct{}

	// Liveness state
	lastHeartBeat time.Time // When this node last received a heartbeat message from the Leader
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return rf.lastSnapshotIndex, rf.lastSnapshotTerm
}

// Returns index within `rf.log` of log entry with index `logIndex`
func (rf *Raft) findLogIndex(logIndex int) (int, bool) {
	for i, e := range rf.log {
		if e.Index == logIndex {
			return i, true
		}
	}
	return -1, false
}

func (rf *Raft) transitionToCandidate() {
	rf.state = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.id
}

func (rf *Raft) transitionToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = ""
}

// InstallSnapshot - RPC function
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.leaderID = args.LeaderId
	}

	if rf.leaderID == args.LeaderId {
		rf.lastHeartBeat = time.Now()
	}

	rf.persister.SaveSnapshot(args.Data) // Save snapshot, discarding any existing snapshot with smaller index

	i, isPresent := rf.findLogIndex(args.LastIncludedIndex)
	if isPresent && rf.log[i].Term == args.LastIncludedTerm {
		// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it
		rf.log = rf.log[i+1:]
	} else { // Otherwise discard the entire log
		rf.log = make([]LogEntry, 0)
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.lastApplied = 0 // LocalApplyProcess will pick this change up and send snapshot

	rf.persist()

	RaftInfo("Installed snapshot from %s, LastSnapshotEntry(Index: %d, Term: %d)", rf, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) sendSnapshot(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	peer := rf.peers[peerIndex]
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.leaderID,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.Unlock()

	// Send RPC (with timeouts + retries)
	requestName := "Raft.InstallSnapshot"
	request := func() bool {
		return peer.Call(requestName, &args, &reply)
	}
	ok := SendRPCRequest(requestName, request)

	rf.Lock()
	defer rf.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.transitionToFollower(reply.Term)
		} else {
			rf.nextIndex[peerIndex] = args.LastIncludedIndex + 1
		}
	}

	sendAppendChan <- struct{}{} // Signal to leader-peer process that there may be appends to send
}

// RequestVote - RPC function
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	lastIndex, lastTerm := rf.getLastEntryInfo()
	logUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}
		return lastTerm < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm && logUpToDate {
		rf.transitionToFollower(args.Term)
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else if (rf.votedFor == "" || args.CandidateID == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	rf.persist()
	RaftInfo("Vote requested for: %s on term: %d. Log up-to-date? %v. Vote granted? %v", rf, args.CandidateID, args.Term, logUpToDate, reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(serverConn *labrpc.ClientEnd, server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	requestName := "Raft.RequestVote"
	request := func() bool {
		return serverConn.Call(requestName, args, reply)
	}

	if ok := SendRPCRequest(requestName, request); ok {
		voteChan <- server
	}
}

// AppendEntries - RPC function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	RaftInfo("Request from %s, w/ %d entries. Args.Prev:[Index %d, Term %d]", rf, args.LeaderID, len(args.LogEntries), args.PreviousLogIndex, args.PreviousLogTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.leaderID = args.LeaderID
	}

	if rf.leaderID == args.LeaderID {
		rf.lastHeartBeat = time.Now()
	}

	// Try to find supplied previous log entry match in our log
	prevLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PreviousLogIndex {
			if v.Term == args.PreviousLogTerm {
				prevLogIndex = i
				break
			} else {
				reply.ConflictingLogTerm = v.Term
			}
		}
	}

	PrevIsInSnapshot := args.PreviousLogIndex == rf.lastSnapshotIndex && args.PreviousLogTerm == rf.lastSnapshotTerm
	PrevIsBeginningOfLog := args.PreviousLogIndex == 0 && args.PreviousLogTerm == 0

	if prevLogIndex >= 0 || PrevIsInSnapshot || PrevIsBeginningOfLog {
		if len(args.LogEntries) > 0 {
			RaftInfo("Appending %d entries from %s", rf, len(args.LogEntries), args.LeaderID)
		}

		// Remove any inconsistent logs and find the index of the last consistent entry from the leader
		entriesIndex := 0
		for i := prevLogIndex + 1; i < len(rf.log); i++ {
			entryConsistent := func() bool {
				localEntry, leadersEntry := rf.log[i], args.LogEntries[entriesIndex]
				return localEntry.Index == leadersEntry.Index && localEntry.Term == leadersEntry.Term
			}
			if entriesIndex >= len(args.LogEntries) || !entryConsistent() {
				// Additional entries must be inconsistent, so let's delete them from our local log
				rf.log = rf.log[:i]
				break
			} else {
				entriesIndex++
			}
		}

		// Append all entries that are not already in our log
		if entriesIndex < len(args.LogEntries) {
			rf.log = append(rf.log, args.LogEntries[entriesIndex:]...)
		}

		// Update the commit index
		if args.LeaderCommit > rf.commitIndex {
			var latestLogIndex = rf.lastSnapshotIndex
			if len(rf.log) > 0 {
				latestLogIndex = rf.log[len(rf.log)-1].Index
			}

			if args.LeaderCommit < latestLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = latestLogIndex
			}
		}
		reply.Success = true
	} else {
		// §5.3: When rejecting an AppendEntries request, the follower can include the term of the
		//	 	 conflicting entry and the first index it stores for that term.

		// If there's no entry with `args.PreviousLogIndex` in our log. Set conflicting term to that of last log entry
		if reply.ConflictingLogTerm == 0 && len(rf.log) > 0 {
			reply.ConflictingLogTerm = rf.log[len(rf.log)-1].Term
		}

		for _, v := range rf.log { // Find first log index for the conflicting term
			if v.Term == reply.ConflictingLogTerm {
				reply.ConflictingLogIndex = v.Index
				break
			}
		}

		reply.Success = false
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	requestName := "Raft.AppendEntries"
	request := func() bool {
		return rf.peers[server].Call(requestName, args, reply)
	}
	return SendRPCRequest(requestName, request)
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
	lastLogIndex, _ := rf.getLastEntryInfo()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {
		if rf.nextIndex[peerIndex] <= rf.lastSnapshotIndex { // We don't have the required entry in our log; sending snapshot.
			rf.Unlock()
			rf.sendSnapshot(peerIndex, sendAppendChan)
			return
		} else {
			for i, v := range rf.log { // Need to send logs beginning from index `rf.nextIndex[peerIndex]`
				if v.Index == rf.nextIndex[peerIndex] {
					if i > 0 {
						lastEntry := rf.log[i-1]
						prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
					} else {
						prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
					}
					entries = make([]LogEntry, len(rf.log)-i)
					copy(entries, rf.log[i:])
					break
				}
			}
			RaftInfo("Sending log %d entries to %s", rf, len(entries), peerId)
		}
	} else { // We're just going to send a heartbeat
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		} else {
			prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
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

	ok := rf.sendAppendEntryRequest(peerIndex, &args, &reply)

	rf.Lock()
	defer rf.Unlock()

	if !ok {
		RaftDebug("Communication error: AppendEntries() RPC failed", rf)
	} else if rf.state != Leader || rf.isDecommissioned || args.Term != rf.currentTerm {
		RaftInfo("Node state has changed since request was sent. Discarding response", rf)
	} else if reply.Success {
		if len(entries) > 0 {
			RaftInfo("Appended %d entries to %s's log", rf, len(entries), peerId)
			lastReplicated := entries[len(entries)-1]
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.updateCommitIndex()
		} else {
			RaftDebug("Successful heartbeat from %s", rf, peerId)
		}
	} else {
		if reply.Term > rf.currentTerm {
			RaftInfo("Switching to follower as %s's term is %d", rf, peerId, reply.Term)
			rf.transitionToFollower(reply.Term)
		} else {
			RaftInfo("Log deviation on %s. T: %d, nextIndex: %d, args.Prev[I: %d, T: %d], FirstConflictEntry[I: %d, T: %d]", rf, peerId, reply.Term, rf.nextIndex[peerIndex], args.PreviousLogIndex, args.PreviousLogTerm, reply.ConflictingLogIndex, reply.ConflictingLogTerm)
			// Log deviation, we should go back to `ConflictingLogIndex - 1`, lowest value for nextIndex[peerIndex] is 1.
			rf.nextIndex[peerIndex] = Max(reply.ConflictingLogIndex-1, 1)
			sendAppendChan <- struct{}{} // Signals to leader-peer process that appends need to occur
		}
	}
	rf.persist()
}

func (rf *Raft) updateCommitIndex() {
	// §5.3/5.4: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := len(rf.log) - 1; i >= 0; i-- {
		if v := rf.log[i]; v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= v.Index {
					if replicationCount++; replicationCount > len(rf.peers)/2 { // Check to see if majority of nodes have replicated this
						RaftInfo("Updating commit index [%d -> %d] as replication factor is at least: %d/%d", rf, rf.commitIndex, v.Index, replicationCount, len(rf.peers))
						rf.commitIndex = v.Index // Set index of this entry as new commit index
						break
					}
				}
			}
		} else {
			break
		}
	}
}

func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	rf.Lock()
	RaftInfo("Starting commit process - Last log applied: %d", rf, rf.lastApplied)
	rf.Unlock()

	for {
		rf.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			if rf.lastApplied < rf.lastSnapshotIndex { // We need to apply latest snapshot
				RaftInfo("Locally applying snapshot with latest index: %d", rf, rf.lastSnapshotIndex)
				rf.Unlock()

				applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot()}

				rf.Lock()
				rf.lastApplied = rf.lastSnapshotIndex
				rf.Unlock()
			} else {
				startIndex, _ := rf.findLogIndex(rf.lastApplied + 1)
				startIndex = Max(startIndex, 0) // If start index wasn't found, it's because it's a part of a snapshot

				endIndex := -1
				for i := startIndex; i < len(rf.log); i++ {
					if rf.log[i].Index <= rf.commitIndex {
						endIndex = i
					}
				}

				if endIndex >= 0 { // We have some entries to locally commit
					entries := make([]LogEntry, endIndex-startIndex+1)
					copy(entries, rf.log[startIndex:endIndex+1])

					RaftInfo("Locally applying %d log entries. lastApplied: %d. commitIndex: %d", rf, len(entries), rf.lastApplied, rf.commitIndex)
					rf.Unlock()

					for _, v := range entries { // Hold no locks so that slow local applies don't deadlock the system
						RaftDebug("Locally applying log: %s", rf, v)
						applyChan <- ApplyMsg{Index: v.Index, Command: v.Command}
					}

					rf.Lock()
					rf.lastApplied += len(entries)
				}
				rf.Unlock()
			}
		} else {
			rf.Unlock()
			<-time.After(CommitApplyIdleCheckInterval)
		}
	}
}

func (rf *Raft) startElectionProcess() {
	electionTimeout := func() time.Duration { // Randomized timeouts between [500, 600)-ms
		return (200 + time.Duration(rand.Intn(300))) * time.Millisecond
	}

	currentTimeout := electionTimeout()
	currentTime := <-time.After(currentTimeout)

	rf.Lock()
	defer rf.Unlock()
	if !rf.isDecommissioned {
		// Start election process if we're not a leader and the haven't received a heartbeat for `electionTimeout`
		if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= currentTimeout {
			RaftInfo("Election timer timed out. Timeout: %fs", rf, currentTimeout.Seconds())
			go rf.beginElection()
		}
		go rf.startElectionProcess()
	}
}

func (rf *Raft) beginElection() {
	rf.Lock()

	rf.transitionToCandidate()
	RaftInfo("Election started", rf)

	// Request votes from peers
	lastIndex, lastTerm := rf.getLastEntryInfo()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(rf.peers[i], i, voteChan, &args, &replies[i])
		}
	}
	rf.persist()
	rf.Unlock()

	// Count votes from peers as they come in
	votes := 1
	for i := 0; i < len(replies); i++ {
		reply := replies[<-voteChan]
		rf.Lock()

		// §5.1: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			RaftInfo("Switching to follower as %s's term is %d", rf, reply.Id, reply.Term)
			rf.transitionToFollower(reply.Term)
			break
		} else if votes += reply.VoteCount(); votes > len(replies)/2 { // Has majority vote
			// Ensure that we're still a candidate and that another election did not interrupt
			if rf.state == Candidate && args.Term == rf.currentTerm {
				RaftInfo("Election won. Vote: %d/%d", rf, votes, len(rf.peers))
				go rf.promoteToLeader()
				break
			} else {
				RaftInfo("Election for term %d was interrupted", rf, args.Term)
				break
			}
		}
		rf.Unlock()
	}
	rf.persist()
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
			rf.matchIndex[i] = 0              // Index of highest log entry known to be replicated on server
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
			return rf.log[len(rf.log)-1].Index + 1
		}
		return Max(1, rf.lastSnapshotIndex+1)
	}()

	entry := LogEntry{Index: nextIndex, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	RaftInfo("New entry appended to leader's log: %s", rf, entry)

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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		id:          string(rune(me + 'A')),
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
	}

	RaftInfo("Node created", rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()
	go rf.startLocalApplyProcess(applyCh)

	return rf
}

// --- Persistence ---

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(
		RaftPersistence{
			CurrentTerm:       rf.currentTerm,
			Log:               rf.log,
			VotedFor:          rf.votedFor,
			LastSnapshotIndex: rf.lastSnapshotIndex,
			LastSnapshotTerm:  rf.lastSnapshotTerm,
		})

	RaftDebug("Persisting node data (%d bytes)", rf, buf.Len())
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	obj := RaftPersistence{}
	d.Decode(&obj)

	rf.votedFor, rf.currentTerm, rf.log = obj.VotedFor, obj.CurrentTerm, obj.Log
	rf.lastSnapshotIndex, rf.lastSnapshotTerm = obj.LastSnapshotIndex, obj.LastSnapshotTerm
	rf.lastApplied = rf.lastSnapshotIndex
	RaftInfo("Loaded persisted node data (%d bytes). Last applied index: %d", rf, len(data), rf.lastApplied)
}

func (rf *Raft) Kill() {
	rf.Lock()
	defer rf.Unlock()

	rf.isDecommissioned = true
	RaftInfo("Node killed", rf)
}

// --- Log compaction ---

func (rf *Raft) CompactLog(lastLogIndex int) {
	rf.Lock()
	defer rf.Unlock()

	if lastLogIndex > rf.commitIndex {
		RaftInfo("Failed to compact log as log index: %d is larger than commit index: %d", rf, lastLogIndex, rf.commitIndex)
	}

	if i, isPresent := rf.findLogIndex(lastLogIndex); isPresent {
		entry := rf.log[i]
		rf.lastSnapshotIndex = entry.Index
		rf.lastSnapshotTerm = entry.Term

		RaftInfo("Compacting log. Removing %d log entries. LastSnapshotEntry(Index: %d, Term: %d)", rf, i+1, entry.Index, entry.Term)
		rf.log = rf.log[i+1:]
	}

	rf.persist()
}
