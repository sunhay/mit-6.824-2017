package raft

import "log"

// Debugging
const Debug = 1

func RaftInfo(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		args := append([]interface{}{rf.id, rf.currentTerm, rf.state}, a...)
		log.Printf("[INFO] Raft: [Id: %s | Term: %d | %v] "+format, args...)
	}
	return
}

func RaftDebug(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		args := append([]interface{}{rf.id, rf.currentTerm, rf.state}, a...)
		log.Printf("[DEBUG] Raft: [Id: %s | Term: %d | %v] "+format, args...)
	}
	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
