package shardkv

import (
	"github.com/sunhay/mit-6.824-2017/shardmaster"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time raft.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const RPCTimeout = 50 * time.Millisecond
const RPCMaxTries = 3

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrMovingShard = "ErrShardMigrating"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	RequestId int64
	ClerkId   int64
}

type GetArgs struct {
	Key       string
	RequestId int64
	ClerkId   int64
}

type RequestReply struct {
	WrongLeader bool
	Err         Err
	Value       string // Optional
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

// SendRPCRequest will keep trying to send an RPC until it succeeds (with timeouts, per request)
func SendRPCRequest(request func() bool) bool {
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
			continue
		}
	}

	return false
}
