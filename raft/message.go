package raft

import "fmt"

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("(I:%d, T: %d)", entry.Index, entry.Term)
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term             int
	LeaderID         string
	PreviousLogIndex int
	PreviousLogTerm  int
	LogEntries       []LogEntry
	LeaderCommit     int
}

type AppendEntriesReply struct {
	Term                int
	Success             bool
	ConflictingLogTerm  int // Term of the conflicting entry, if any
	ConflictingLogIndex int // First index of the log for the above conflicting term
}

// RequestVote RPC
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Id          string
}

func (reply *RequestVoteReply) VoteCount() int {
	if reply.VoteGranted {
		return 1
	}
	return 0
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// RaftPersistence is persisted to the `persister`, and contains all necessary data to restart a failed node
type RaftPersistence struct {
	CurrentTerm       int
	Log               []LogEntry
	VotedFor          string
	LastSnapshotIndex int
	LastSnapshotTerm  int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}
