package raftkv

import (
	"encoding/gob"
	"github.com/sunhay/scratchpad/golang/mit-6.824-2017/src/labrpc"
	"github.com/sunhay/scratchpad/golang/mit-6.824-2017/src/raft"
	"log"
	"sync"
	"time"
)

const Debug = 1

func RaftKVInfo(format string, kv *RaftKV, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		args := append([]interface{}{kv.id, len(kv.data)}, a...)
		log.Printf("[INFO] KV Server: [Id: %s, Size: %d] "+format, args...)
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
	Command   CommandType
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type RaftKV struct {
	sync.Mutex

	me      int
	id      string
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate    int // snapshot if log grows this big
	isDecomissioned bool

	requestHandlers map[int]chan raft.ApplyMsg
	data            map[string]string
	latestRequests  map[int64]int64 // Client ID -> Last applied Request ID
}

// Returns whether or not this was a successful request
func (kv *RaftKV) await(index int, op Op) (success bool) {
	kv.Lock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.requestHandlers[index] = awaitChan
	kv.Unlock()

	for {
		select {
		case message := <-awaitChan:
			kv.Lock()
			delete(kv.requestHandlers, index)
			kv.Unlock()

			if index == message.Index && op == message.Command {
				return true
			} else { // Message at index was not what we're expecting, must not be leader in majority partition
				return false
			}
		case <-time.After(10 * time.Millisecond):
			kv.Lock()
			if _, stillLeader := kv.rf.GetState(); !stillLeader { // We're no longer leader. Abort
				delete(kv.requestHandlers, index)
				kv.Unlock()
				return false
			}
			kv.Unlock()
		}
	}

}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Command: Get, Key: args.Key, ClientId: args.ClerkId, RequestId: args.RequestId}

	kv.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, op)
		if !success { // Request likely failed due to leadership change
			reply.WrongLeader = true
			RaftKVInfo("Get(): Failed, node is no longer leader", kv)
		} else {
			kv.Lock()
			if val, isPresent := kv.data[args.Key]; isPresent {
				RaftKVInfo("Get(): Succeeded for key: %s", kv, args.Key)
				reply.Err = OK
				reply.Value = val
			} else {
				RaftKVInfo("Get(): Failed, no entry for key: %s", kv, args.Key)
				reply.Err = ErrNoKey
			}
			kv.Unlock()
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Key: args.Key, Value: args.Value, ClientId: args.ClerkId, RequestId: args.RequestId}
	if args.Op == "Put" {
		op.Command = Put
	} else {
		op.Command = Append
	}

	kv.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := kv.await(index, op)
		if !success { // Request likely failed due to leadership change
			RaftKVInfo("%s(): Failed, node is no longer leader", kv, args.Op)
			reply.WrongLeader = true
		} else {
			RaftKVInfo("%s(): Succeeded for key: %s", kv, args.Op, args.Key)
			reply.Err = OK
		}
	}
}

func (kv *RaftKV) startApplyProcess() {
	RaftKVInfo("Starting apply process", kv)
	for !kv.isDecomissioned {
		select {
		case m := <-kv.applyCh:
			kv.Lock()

			op := m.Command.(Op)

			// Deduplicate write requests: Each RPC implies that the client has seen the reply for its previous RPC.
			//                      	   It's OK to assume that a client will make only one call into a clerk at a time.
			if op.Command != Get {
				if requestId, isPresent := kv.latestRequests[op.ClientId]; !(isPresent && requestId == op.RequestId) {
					if op.Command == Put {
						kv.data[op.Key] = op.Value
					} else if op.Command == Append {
						kv.data[op.Key] += op.Value
					}
					kv.latestRequests[op.ClientId] = op.RequestId
				} else {
					RaftKVInfo("Write request de-duplicated for key: %s. RId: %d, CId: %d", kv, op.Key, op.RequestId, op.ClientId)
				}
			}

			if c, isPresent := kv.requestHandlers[m.Index]; isPresent {
				c <- m // TODO: Should probably send value if Get, likely false linearizability due to race conditions
			}

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
	kv.isDecomissioned = true
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

	kv := RaftKV{
		me:              me,
		id:              string('Z' - me),
		maxraftstate:    maxraftstate,
		requestHandlers: make(map[int]chan raft.ApplyMsg),
		data:            make(map[string]string),
		latestRequests:  make(map[int64]int64),
		applyCh:         make(chan raft.ApplyMsg),
	}

	RaftKVInfo("Starting node", &kv)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.startApplyProcess()

	return &kv
}
