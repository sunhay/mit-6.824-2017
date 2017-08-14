package shardmaster

import "github.com/sunhay/mit-6.824-2017/raft"
import "github.com/sunhay/mit-6.824-2017/labrpc"
import "sync"
import (
	"encoding/gob"
	"log"
	"reflect"
	"time"
)

const AwaitLeaderCheckInterval = 10 * time.Millisecond
const Debug = 1
const LogMasterOnly = true

func smInfo(format string, sm *ShardMaster, a ...interface{}) (n int, err error) {
	if _, isLeader := sm.rf.GetState(); LogMasterOnly && !isLeader {
		return
	}
	if Debug > 0 {
		args := append([]interface{}{sm.me, len(sm.configs)}, a...)
		log.Printf("[INFO] Shard Master: [Id: %d, %d configs] "+format, args...)
	}
	return
}

type CommandType int

const (
	Join CommandType = iota
	Leave
	Move
	Query
)

type Arguments interface{}

type ShardMaster struct {
	sync.Mutex

	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	isDecommissioned bool

	requestHandlers map[int]chan raft.ApplyMsg
	configs         []Config        // indexed by config num
	latestRequests  map[int64]int64 // Client ID -> Last applied Request ID
}

type Op struct {
	Command CommandType
	Args    Arguments
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *RequestReply) {
	sm.startRequest(Op{Command: Join, Args: *args}, reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *RequestReply) {
	sm.startRequest(Op{Command: Leave, Args: *args}, reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *RequestReply) {
	sm.startRequest(Op{Command: Move, Args: *args}, reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *RequestReply) {
	sm.startRequest(Op{Command: Query, Args: *args}, reply)

	sm.Lock()
	defer sm.Unlock()

	if reply.Err == OK {
		if args.Num < 0 || args.Num >= len(sm.configs) {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
	}
}

func (sm *ShardMaster) startRequest(op Op, reply *RequestReply) {
	sm.Lock()
	index, _, isLeader := sm.rf.Start(op)
	sm.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := sm.awaitApply(index, op)
		if !success { // Request likely failed due to leadership change
			reply.WrongLeader = true
			smInfo("Request failed, node is no longer leader", sm)
		} else {
			sm.Lock()
			reply.Err = OK
			sm.Unlock()
		}
	}
}

// Returns whether or not this was a successful request
func (sm *ShardMaster) awaitApply(index int, op Op) (success bool) {
	sm.Lock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	sm.requestHandlers[index] = awaitChan
	sm.Unlock()

	for {
		select {
		case message := <-awaitChan:
			sm.Lock()
			delete(sm.requestHandlers, index)
			sm.Unlock()

			if index == message.Index && reflect.DeepEqual(op, message.Command) {
				return true
			} else { // Message at index was not what we're expecting, must not be leader in majority partition
				return false
			}
		case <-time.After(AwaitLeaderCheckInterval):
			sm.Lock()
			if _, stillLeader := sm.rf.GetState(); !stillLeader { // We're no longer leader. Abort
				delete(sm.requestHandlers, index)
				sm.Unlock()
				return false
			}
			sm.Unlock()
		}
	}
}

func (sm *ShardMaster) startApplyProcess() {
	smInfo("Starting apply process", sm)

	for {
		select {
		case m := <-sm.applyCh:
			sm.Lock()

			if sm.isDecommissioned {
				sm.Unlock()
				return
			}

			switch op := m.Command.(Op); op.Command {
			case Join:
				sm.applyJoin(op.Args.(JoinArgs))
			case Leave:
				sm.applyLeave(op.Args.(LeaveArgs))
			case Move:
				sm.applyMove(op.Args.(MoveArgs))
			case Query: // Read only, no need to modify local state
				break
			default:
				smInfo("Unknown command type %s", sm, op.Command)
			}

			if c, isPresent := sm.requestHandlers[m.Index]; isPresent {
				c <- m
			}

			sm.Unlock()
		}
	}
}

// De-duplicating requests, for "exactly-once" semantics
// Note: Each RPC implies that the client has seen the reply for its previous RPC. It's OK to assume that
// a client will make only one call into a clerk at a time.
func (sm *ShardMaster) isRequestDuplicate(clientId int64, requestId int64) bool {
	lastRequest, isPresent := sm.latestRequests[clientId]
	sm.latestRequests[clientId] = requestId
	return isPresent && lastRequest == requestId
}

func (sm *ShardMaster) applyJoin(args JoinArgs) {
	if sm.isRequestDuplicate(args.ClientId, args.RequestId) {
		smInfo("Request de-duplicated for: Join{%s}", sm, args)
		return
	}

	smInfo("Applying Join{%s}", sm, args)

	newConfig := sm.configs[len(sm.configs)-1].Clone()
	newConfig.Num += 1

	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyLeave(args LeaveArgs) {
	if sm.isRequestDuplicate(args.ClientId, args.RequestId) {
		smInfo("Request de-duplicated for: Leave{%s}", sm, args)
		return
	}

	smInfo("Applying Leave{%s}", sm, args)

	newConfig := sm.configs[len(sm.configs)-1].Clone()
	newConfig.Num += 1

	for _, gid := range args.GIDs {
		for shardNum, assignedGID := range newConfig.Shards {
			if gid == assignedGID {
				newConfig.Shards[shardNum] = 0 // Assign to invalid GID
			}
			delete(newConfig.Groups, gid)
		}
	}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyMove(args MoveArgs) {
	if sm.isRequestDuplicate(args.ClientId, args.RequestId) {
		smInfo("Request de-duplicated for: Move{%s}", sm, args)
		return
	}

	smInfo("Applying Move{%s}", sm, args)

	newConfig := sm.configs[len(sm.configs)-1].Clone()
	newConfig.Num += 1
	newConfig.Shards[args.Shard] = args.GID

	sm.configs = append(sm.configs, newConfig)
}

// Note: This is a tad gnarly.
func (sm *ShardMaster) rebalanceShards(config *Config) {
	minShardsPerGroup := NShards / len(config.Groups) // Minimum shard count per group member

	distribution := make(map[int][]int) // Find the current distribution of GID -> shards
	unassignedShards := make([]int, 0)

	for gid := range config.Groups {
		distribution[gid] = make([]int, 0)
	}

	for shard, gid := range config.Shards {
		if gid != 0 {
			distribution[gid] = append(distribution[gid], shard)
		} else {
			unassignedShards = append(unassignedShards, shard)
		}
	}

	areShardsBalanced := func() bool { // Helper to determine if all shards are assigned + balanced across groups
		sum := 0
		for gid := range distribution {
			shardCount := len(distribution[gid])
			if shardCount < minShardsPerGroup {
				return false
			}
			sum += shardCount
		}
		return sum == NShards
	}

	// Distribute shards such that they're balanced across groups
	for !areShardsBalanced() {
		for gid := range distribution {
			// If this group is under capacity, add as many shards as we can to it from the unassigned
			for len(unassignedShards) > 0 && len(distribution[gid]) < minShardsPerGroup {
				distribution[gid] = append(distribution[gid], unassignedShards[0])
				unassignedShards = unassignedShards[1:]
			}

			// If there aren't any unassigned shards and group is under-capacity, "steal" shard from an over-capacity group
			if len(unassignedShards) == 0 && len(distribution[gid]) < minShardsPerGroup {
				for gid2 := range distribution {
					if len(distribution[gid2]) > minShardsPerGroup {
						distribution[gid] = append(distribution[gid], distribution[gid2][0])
						distribution[gid2] = distribution[gid2][1:]
						break
					}
				}
			}
		}

		// If we still have unassigned shards, assign them one by one to each group
		for gid := range distribution {
			if len(unassignedShards) > 0 {
				distribution[gid] = append(distribution[gid], unassignedShards[0])
				unassignedShards = unassignedShards[1:]
			}
		}
	}

	// Assign distribution to config
	for gid, shards := range distribution {
		for _, i := range shards {
			config.Shards[i] = gid
		}
	}
}

func (sm *ShardMaster) Kill() {
	sm.Lock()
	defer sm.Unlock()
	sm.rf.Kill()
	sm.isDecommissioned = true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := ShardMaster{
		me:              me,
		configs:         make([]Config, 1),
		requestHandlers: make(map[int]chan raft.ApplyMsg),
		latestRequests:  make(map[int64]int64),
	}

	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(MoveArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.startApplyProcess()

	smInfo("Started node", &sm)

	return &sm
}
