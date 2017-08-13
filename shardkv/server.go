package shardkv

import "github.com/sunhay/mit-6.824-2017/labrpc"
import "github.com/sunhay/mit-6.824-2017/raft"
import "github.com/sunhay/mit-6.824-2017/shardmaster"
import "sync"
import (
	"encoding/gob"
	"log"
	"reflect"
	"strconv"
	"time"
)

const AwaitLeaderCheckInterval = 10 * time.Millisecond
const ShardMasterCheckInterval = 50 * time.Millisecond
const Debug = 1

func kvInfo(format string, kv *ShardKV, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		args := append([]interface{}{kv.gid, kv.StateDescription(), len(kv.data)}, a...)
		log.Printf("[INFO] KV Server: [GId: %d, %s, %d shards] "+format, args...)
	}
	return
}

func kvDebug(format string, kv *ShardKV, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		args := append([]interface{}{kv.gid, kv.StateDescription(), len(kv.data)}, a...)
		log.Printf("[DEBUG] KV Server: [GId: %d, %s, %d shards] "+format, args...)
	}
	return
}

type CommandType int

const (
	Put CommandType = iota
	Append
	Get
	ConfigUpdate
)

type Op interface {
	getCommand() CommandType
}

type ClientOp struct {
	Command   CommandType
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

func (op ClientOp) getCommand() CommandType {
	return op.Command
}

type ConfigOp struct {
	Command CommandType
	Config  shardmaster.Config
}

func (op ConfigOp) getCommand() CommandType {
	return op.Command
}

type ShardKV struct {
	sync.Mutex

	me  int
	id  string
	gid int

	sm      *shardmaster.Clerk
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	make_end func(string) *labrpc.ClientEnd
	masters  []*labrpc.ClientEnd

	maxraftstate     int // snapshot if log grows this big
	isDecommissioned bool

	requestHandlers map[int]chan raft.ApplyMsg

	// Persistent storage
	latestConfig   shardmaster.Config
	data           map[int]map[string]string // Partition ID (int) -> Shard key-values (string -> string)
	latestRequests map[int64]int64           // Client ID -> Last applied Request ID
}

func (kv *ShardKV) groupForKey(key string) int {
	return kv.latestConfig.Shards[key2shard(key)]
}

func (kv *ShardKV) Get(args *GetArgs, reply *RequestReply) {
	op := ClientOp{Command: Get, Key: args.Key, ClientId: args.ClerkId, RequestId: args.RequestId}

	kv.startClientRequest(op, reply)

	if reply.Err != OK {
		if !reply.WrongLeader {
			kvDebug("Get(): Failed for key: %s, Error: %s", kv, args.Key, reply.Err)
		}
		return
	}

	kv.Lock()
	defer kv.Unlock()
	kvInfo("Get(): Key %s, shard: %d - Succeeded", kv, args.Key, key2shard(args.Key))
	if val, isPresent := kv.data[key2shard(args.Key)][args.Key]; isPresent {
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *RequestReply) {
	op := ClientOp{Key: args.Key, Value: args.Value, ClientId: args.ClerkId, RequestId: args.RequestId}
	if args.Op == "Put" {
		op.Command = Put
	} else {
		op.Command = Append
	}

	kv.startClientRequest(op, reply)

	if reply.Err != OK {
		kvDebug("%s(): Key: %s, shard: %d - Failed", kv, args.Op, args.Key, key2shard(args.Key))
	} else {
		kvInfo("%s(): Key: %s, shard: %d - Succeeded", kv, args.Op, args.Key, key2shard(args.Key))
	}
}

func (kv *ShardKV) startClientRequest(op ClientOp, reply *RequestReply) {
	kv.Lock()
	if gid := kv.groupForKey(op.Key); gid != kv.gid {
		reply.Err = ErrWrongGroup
		reply.Value = strconv.Itoa(gid)
		kv.Unlock()
		return
	}
	kv.Unlock()

	kv.startRequest(op, reply)

	if !reply.WrongLeader {
		kv.Lock()
		if gid := kv.groupForKey(op.Key); gid != kv.gid {
			reply.Err = ErrWrongGroup
			reply.Value = strconv.Itoa(gid)
			kvInfo("Group no longer owns shard (due to config change)", kv)
		} else {
			reply.Err = OK
		}
		kv.Unlock()
	}
}

func (kv *ShardKV) startRequest(op Op, reply *RequestReply) {
	kv.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		if success := kv.awaitApply(index, op); !success { // Request likely failed due to leadership change
			reply.WrongLeader = true
		}
	}
}

// Returns whether or not this was a successful request
func (kv *ShardKV) awaitApply(index int, op Op) (success bool) {
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

			if index == message.Index && reflect.DeepEqual(op, message.Command) {
				return true
			} else { // Message at index was not what we're expecting, must not be leader in majority partition
				return false
			}
		case <-time.After(AwaitLeaderCheckInterval):
			kv.Lock()
			if _, stillLeader := kv.rf.GetState(); !stillLeader || kv.isDecommissioned { // We're no longer leader. Abort
				delete(kv.requestHandlers, index)
				kv.Unlock()
				return false
			}
			kv.Unlock()
		}
	}
}

func (kv *ShardKV) startApplyProcess() {
	kvInfo("Starting apply process", kv)
	for {
		select {
		case m := <-kv.applyCh:
			kv.Lock()

			if kv.isDecommissioned {
				kv.Unlock()
				return
			}

			switch op := m.Command.(type) {
			case ClientOp:
				kv.applyClientOperation(op)
			case ConfigOp:
				kv.applyConfigOperation(op)
			}

			if c, isPresent := kv.requestHandlers[m.Index]; isPresent {
				c <- m
			}

			kv.Unlock()
		}
	}
}

func (kv *ShardKV) applyClientOperation(op ClientOp) {
	if !kv.isRequestDuplicate(op.ClientId, op.RequestId) && op.Command != Get {
		// Double check that shard exists on this node, then write
		if shardData, shardPresent := kv.data[key2shard(op.Key)]; shardPresent {
			if op.Command == Put {
				shardData[op.Key] = op.Value
			} else if op.Command == Append {
				shardData[op.Key] += op.Value
			}
		}
		kv.latestRequests[op.ClientId] = op.RequestId
	}
}

// De-duplicating requests, for "exactly-once" semantics
// Note: Each RPC implies that the client has seen the reply for its previous RPC. It's OK to assume that
// a client will make only one call into a clerk at a time.
func (kv *ShardKV) isRequestDuplicate(clientId int64, requestId int64) bool {
	lastRequest, isPresent := kv.latestRequests[clientId]
	kv.latestRequests[clientId] = requestId
	return isPresent && lastRequest == requestId
}

func (kv *ShardKV) applyConfigOperation(op ConfigOp) {
	existingShardCount := len(kv.data)

	for shardNum, gid := range op.Config.Shards {
		if _, shardPresent := kv.data[shardNum]; gid == kv.gid && !shardPresent {
			kv.data[shardNum] = make(map[string]string) // Creating shard
		} else if gid != kv.gid && shardPresent {
			delete(kv.data, shardNum) // Deleting shard
		}
	}

	if len(kv.data) != existingShardCount {
		shards := make([]int, 0)
		for k := range kv.data {
			shards = append(shards, k)
		}
		kvInfo("Configuration change, shard count: [%d -> %d], shards owned: %v", kv, existingShardCount, len(kv.data), shards)
	}

	kv.latestConfig = op.Config
}

func (kv *ShardKV) startConfigListener() {
	kvInfo("Starting config listener", kv)
	ticker := time.NewTicker(ShardMasterCheckInterval)

	for {
		select {
		case <-ticker.C:
			if kv.isDecommissioned {
				return
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				config := kv.sm.Query(-1)
				kv.Lock()
				if kv.latestConfig.Num < config.Num {
					op := ConfigOp{Command: ConfigUpdate, Config: config}
					kv.Unlock()
					kv.startRequest(op, &RequestReply{})
					continue
				}
				kv.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.Lock()
	defer kv.Unlock()

	kv.isDecommissioned = true
	kv.rf.Kill()
}

func (kv *ShardKV) StateDescription() string {
	if kv.rf == nil {
		return "Replica"
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		return "Master"
	} else {
		return "Replica"
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want Go's RPC library to marshall/unmarshall.
	gob.Register(ClientOp{})
	gob.Register(ConfigOp{})

	kv := ShardKV{
		me:              me,
		maxraftstate:    maxraftstate,
		make_end:        make_end,
		gid:             gid,
		masters:         masters,
		applyCh:         make(chan raft.ApplyMsg),
		requestHandlers: make(map[int]chan raft.ApplyMsg),
		data:            make(map[int]map[string]string),
		latestRequests:  make(map[int64]int64),
	}

	kvInfo("Starting group: %d, on node: %d", &kv, gid, me)

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.startConfigListener()
	go kv.startApplyProcess()

	return &kv
}
