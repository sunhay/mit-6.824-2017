package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 0

const RPCTimeout = 50 * time.Millisecond
const RPCMaxTries = 3

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

func RPCDebug(format string, svcName string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		args := append([]interface{}{svcName}, a...)
		log.Printf("[DEBUG] RPC: [%s] "+format, args...)
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

// SendRPCRequest will attempt a request `RPCMaxTries` tries with a timeout of `RPCTimeout` each time before giving up
func SendRPCRequest(requestName string, request func() bool) bool {
	makeRequest := func(successChan chan struct{}) {
		if ok := request(); ok {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < RPCMaxTries; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)
		select {
		case <-rpcChan:
			return true
		case <-time.After(RPCTimeout):
			RPCDebug("Request attempt %d timed out", requestName, attempts+1)
		}
	}

	RPCDebug("Request failed", requestName)
	return false
}
