package raftkv

import (
	"encoding/gob"
	"github.com/sunhay/scratchpad/golang/mit-6.824-2017/src/labrpc"
	"github.com/sunhay/scratchpad/golang/mit-6.824-2017/src/raft"
	"log"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type CommandType int

const (
	Put CommandType = iota
	Append
	Get
)

type Op struct {
	Command CommandType
	Value   string
	Id      string // Request ID : TODO
}

type RaftKV struct {
	sync.Mutex

	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	inFlight map[int]chan raft.ApplyMsg

	data map[string]string
}

// Returns whether or not this was a successful request
func (kv *RaftKV) await(index int, op Op) (success bool) {
	kv.Lock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.inFlight[index] = awaitChan
	kv.Unlock()

	for {
		select {
		case message := <-awaitChan:
			kv.Lock()
			delete(kv.inFlight, index)
			kv.Unlock()

			if index == message.Index && op == message.Command {
				return true
			} else { // Must not have been the leader in the majority partition?
				return false
			}
		case <-time.After(10 * time.Millisecond):
			kv.Lock()
			if _, stillLeader := kv.rf.GetState(); !stillLeader { // We're no longer leader. Abort
				delete(kv.inFlight, index)
				kv.Unlock()
				return false
			}
			kv.Unlock()
		}
	}

}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Command: Get, Value: args.Key}

	kv.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, op)
		if !success { // Request likely failed due to leadership change
			reply.WrongLeader = true
		} else {
			kv.Lock()
			if val, isPresent := kv.data[args.Key]; isPresent {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
			}
			kv.Unlock()
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := func() Op {
		if args.Op == "Put" {
			return Op{Command: Put, Value: args.Value}
		}
		return Op{Command: Append, Value: args.Value}
	}()

	kv.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, op)
		if !success { // Request likely failed due to leadership change
			reply.WrongLeader = true
		} else {
			kv.Lock()
			if op.Command == Put {
				kv.data[args.Key] = args.Value
			} else {
				kv.data[args.Key] += args.Value
			}
			reply.Err = OK
			kv.Unlock()
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
