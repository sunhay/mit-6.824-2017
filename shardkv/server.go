package shardkv

import "github.com/sunhay/mit-6.824-2017/labrpc"
import "github.com/sunhay/mit-6.824-2017/raft"
import "github.com/sunhay/mit-6.824-2017/shardmaster"
import "sync"
import (
	"bytes"
	"encoding/gob"
	"log"
	"reflect"
	"strconv"
	"time"
)

const StatusUpdateInterval = 500 * time.Millisecond
const AwaitLeaderCheckInterval = 10 * time.Millisecond
const ShardMasterCheckInterval = 50 * time.Millisecond
const SnapshotSizeTolerancePercentage = 5

const Debug = 1
const LogMasterOnly = true

func kvInfo(format string, kv *ShardKV, a ...interface{}) {
	state := kv.StateDescription()
	if LogMasterOnly && state != "Master" {
		return
	}
	if Debug > 0 {
		args := append([]interface{}{kv.gid, kv.me, state, kv.latestConfig.Num}, a...)
		log.Printf("[INFO] KV Server: [GId: %d, Id: %d, %s, Config: %d] "+format, args...)
	}
}

func kvDebug(format string, kv *ShardKV, a ...interface{}) {
	state := kv.StateDescription()
	if LogMasterOnly && state != "Master" {
		return
	}
	if Debug > 1 {
		args := append([]interface{}{kv.gid, kv.me, state, kv.latestConfig.Num}, a...)
		log.Printf("[DEBUG] KV Server: [GId: %d, Id: %d, %s, Config: %d] "+format, args...)
	}
}

type CommandType int

const (
	Put CommandType = iota
	Append
	Get
	ConfigUpdate
	ShardTransfer
	MigrationComplete
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

type ShardTransferOp struct {
	Command        CommandType
	Num            int
	Shard          map[string]string
	LatestRequests map[int64]int64
}

func (op ShardTransferOp) getCommand() CommandType {
	return op.Command
}

type ShardState string

const (
	NotStored     ShardState = "NotStored"
	Available                = "Avail"
	MigratingData            = "Migrate"
	AwaitingData             = "Await"
)

type SendShardArgs struct {
	Num        int
	Data       map[string]string
	LatestReqs map[int64]int64
	Config     shardmaster.Config
}

type SendShardReply struct {
	IsLeader bool
	Err      Err
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
	persister        *raft.Persister
	snapshotsEnabled bool
	isDecommissioned bool

	requestHandlers map[int]chan raft.ApplyMsg

	// Persistent storage
	shardStatus    map[int]ShardState
	latestConfig   shardmaster.Config
	data           map[int]map[string]string // Shard ID (int) -> Shard key-values (string -> string)
	latestRequests map[int]map[int64]int64   // Shard ID (int) -> Last request key-values (Client ID -> Request ID)
}

type ShardKVPersistence struct {
	LatestConfig   shardmaster.Config
	ShardStatus    map[int]ShardState
	Data           map[int]map[string]string
	LatestRequests map[int]map[int64]int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *RequestReply) {
	op := ClientOp{Command: Get, Key: args.Key, ClientId: args.ClerkId, RequestId: args.RequestId}

	kv.startClientRequest(op, reply)

	if reply.Err != OK || reply.WrongLeader {
		kvDebug("Get(): Key: %s, shard: %d - Error: %s", kv, args.Key, key2shard(args.Key), reply.Err)
		return
	}

	kv.Lock()
	defer kv.Unlock()
	kvInfo("Get(): Key: %s, shard: %d - Succeeded", kv, args.Key, key2shard(args.Key))
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

	if reply.Err != OK || reply.WrongLeader {
		kvDebug("%s(): Key: %s, shard: %d - Failed", kv, args.Op, args.Key, key2shard(args.Key))
	} else {
		kvInfo("%s(): Key: %s, shard: %d - Succeeded", kv, args.Op, args.Key, key2shard(args.Key))
	}
}

func (kv *ShardKV) startClientRequest(op ClientOp, reply *RequestReply) {
	setError := func() {
		kv.Lock()
		switch kv.shardStatus[key2shard(op.Key)] {
		case NotStored:
			fallthrough
		case MigratingData:
			reply.Err = ErrWrongGroup
			reply.Value = strconv.Itoa(kv.latestConfig.Shards[key2shard(op.Key)])
		case AwaitingData:
			reply.Err = ErrMovingShard
		case Available:
			reply.Err = OK
		}
		kv.Unlock()
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	} else if setError(); reply.Err != OK {
		return
	}

	kv.startRequest(op, reply)

	if !reply.WrongLeader {
		setError()
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

			if m.UseSnapshot { // ApplyMsg might be a request to load snapshot
				kv.loadSnapshot(m.Snapshot)
				kv.Unlock()
				continue
			}

			switch op := m.Command.(type) {
			case ClientOp:
				kv.applyClientOp(op)
			case ConfigOp:
				kv.applyConfigOp(op)
			case ShardTransferOp:
				kv.applyShardTransferOp(op)
			}

			if c, isPresent := kv.requestHandlers[m.Index]; isPresent {
				c <- m
			}

			// Create snapshot if log is close to reaching max size (within `SnapshotSizeTolerancePercentage`)
			if kv.snapshotsEnabled && 1-(kv.persister.RaftStateSize()/kv.maxraftstate) <= SnapshotSizeTolerancePercentage/100 {
				kvInfo("Creating snapshot. Raft state size: %d bytes, Max size = %d bytes", kv, kv.persister.RaftStateSize(), kv.maxraftstate)
				kv.createSnapshot(m.Index)
			}

			kv.Unlock()
		}
	}
}

// De-duplicating requests, for "exactly-once" semantics
// Note: Each RPC implies that the client has seen the reply for its previous RPC. It's OK to assume that
// a client will make only one call into a clerk at a time.
func (kv *ShardKV) isRequestDuplicate(shard int, clientId int64, requestId int64) bool {
	shardRequests, shardPresent := kv.latestRequests[shard]
	if shardPresent {
		lastRequest, isPresent := shardRequests[clientId]
		return isPresent && lastRequest == requestId
	}
	return false
}

func (kv *ShardKV) applyClientOp(op ClientOp) {
	if !kv.isRequestDuplicate(key2shard(op.Key), op.ClientId, op.RequestId) && op.Command != Get {
		// Double check that shard exists on this node, then write
		if shardData, shardPresent := kv.data[key2shard(op.Key)]; shardPresent {
			if op.Command == Put {
				shardData[op.Key] = op.Value
			} else if op.Command == Append {
				shardData[op.Key] += op.Value
			}
			kv.latestRequests[key2shard(op.Key)][op.ClientId] = op.RequestId // Safe since shard exists in `kv.data`
		}
	}
}

func (kv *ShardKV) applyConfigOp(op ConfigOp) {
	shardTransferInProgress := func() bool {
		for _, status := range kv.shardStatus {
			if status == MigratingData || status == AwaitingData {
				return true
			}
		}
		return false
	}()

	// We want to apply configurations in-order and when previous transfers are done
	if op.Config.Num == kv.latestConfig.Num+1 && !shardTransferInProgress {
		kv.latestConfig = op.Config
		for shard, gid := range op.Config.Shards {
			shardStatus := kv.shardStatus[shard]
			if gid == kv.gid && shardStatus == NotStored {
				kvInfo("Configuration change: Now owner of shard %d", kv, shard)
				kv.shardStatus[shard] = AwaitingData
				go kv.createShardIfNeeded(shard, op.Config)
			} else if gid != kv.gid && shardStatus == Available {
				kvInfo("Configuration change: No longer owner of shard %d", kv, shard)
				kv.shardStatus[shard] = MigratingData
				go kv.deleteShard(shard, op.Config)
			}
		}
	}
}

func (kv *ShardKV) createShardIfNeeded(shardNum int, config shardmaster.Config) {
	createNewShard := func() {
		kv.Lock()
		defer kv.Unlock()
		kv.shardStatus[shardNum] = Available
		kv.data[shardNum] = make(map[string]string)
		kv.latestRequests[shardNum] = make(map[int64]int64)
	}

	// Query previous configurations until we find either there was a previous owner, or that we're the first owner
	lastConfig := config
	for lastConfig.Num > 1 && lastConfig.Shards[shardNum] == kv.gid {
		lastConfig = kv.sm.Query(lastConfig.Num - 1)
	}

	if lastConfig.Num == 1 && lastConfig.Shards[shardNum] == kv.gid { // If this is the first config, and we're the owner
		kvInfo("Creating new shard: %d", kv, shardNum)
		createNewShard()
		return
	} else {
		kvInfo("Awaiting data for shard: %d from %d", kv, shardNum, lastConfig.Shards[shardNum])
	}
}

func (kv *ShardKV) deleteShard(shardNum int, config shardmaster.Config) {
	if _, isLeader := kv.rf.GetState(); !isLeader { // Only the leader should be moving shards
		return
	}

	for {
		servers := config.Groups[config.Shards[shardNum]]

		kv.Lock()
		args := SendShardArgs{Num: shardNum, Data: kv.data[shardNum], LatestReqs: kv.latestRequests[shardNum], Config: config}
		reply := SendShardReply{}
		kv.Unlock()

		for i := 0; !reply.IsLeader; i++ {
			clientEnd := servers[i%len(servers)]
			request := func() bool {
				kvInfo("Sending shard %d to client: %s", kv, shardNum, clientEnd)
				return kv.make_end(clientEnd).Call("ShardKV.SendShard", &args, &reply)
			}
			SendRPCRequest(request)
		}

		if reply.Err == OK {
			kvInfo("Shard: %d successfully transferred", kv, shardNum)
			kv.startRequest(ShardTransferOp{Command: MigrationComplete, Num: shardNum}, &RequestReply{})
			break
		}
	}
}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.IsLeader = false
		return
	}

	reply.IsLeader = true

	kv.Lock()
	if kv.latestConfig.Num == args.Config.Num {
		// Copy shard data
		data := make(map[string]string)
		for k := range args.Data {
			data[k] = args.Data[k]
		}

		// Copy shard's latest requests
		latestRequests := make(map[int64]int64)
		for k := range args.LatestReqs {
			latestRequests[k] = args.LatestReqs[k]
		}

		op := ShardTransferOp{Command: ShardTransfer, Num: args.Num, Shard: data, LatestRequests: latestRequests}
		go kv.startRequest(op, &RequestReply{})

		reply.Err = OK
	} else if args.Config.Num < kv.latestConfig.Num { // Old config and we've likely already handled this
		reply.Err = OK
	}
	kv.Unlock()
}

func (kv *ShardKV) applyShardTransferOp(op ShardTransferOp) {
	switch op.Command {
	case MigrationComplete:
		delete(kv.data, op.Num)
		delete(kv.latestRequests, op.Num)
		kv.shardStatus[op.Num] = NotStored
	case ShardTransfer:
		if kv.shardStatus[op.Num] == AwaitingData {
			kv.data[op.Num] = op.Shard
			kv.latestRequests[op.Num] = op.LatestRequests
			kv.shardStatus[op.Num] = Available
			kvInfo("Data for shard: %d successfully received", kv, op.Num)
		}
	}
}

func (kv *ShardKV) startConfigListener() {
	kvInfo("Starting config listener", kv)

	for {
		select {
		case <-time.After(ShardMasterCheckInterval):
			if kv.isDecommissioned {
				return
			}

			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}

			config := kv.sm.Query(-1) // Getting latest configuration

			kv.Lock()
			if latestConfigNum := kv.latestConfig.Num; latestConfigNum < config.Num {
				kv.Unlock()

				kvInfo("Batch applying configs from %d -> %d", kv, latestConfigNum, config.Num)

				// Get all configs from last applied config to the config received and apply them in order
				for i := 0; i < config.Num-latestConfigNum; i++ {
					currentConfigNum := latestConfigNum + i + 1

					var c shardmaster.Config
					if currentConfigNum == config.Num { // Did we already fetch this one?
						c = config
					} else {
						c = kv.sm.Query(currentConfigNum)
					}

					// Apply the configs in-order.
					reply := RequestReply{}
					for reply.Err != OK {
						op := ConfigOp{Command: ConfigUpdate, Config: c}
						kv.startRequest(op, &reply)
						kv.Lock()
						if kv.latestConfig.Num == c.Num {
							reply.Err = OK
						}
						kv.Unlock()
					}
				}
			} else {
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
	if kv.rf != nil {
		if _, isLeader := kv.rf.GetState(); isLeader {
			return "Master"
		}
	}
	return "Replica"
}

func (kv *ShardKV) createSnapshot(logIndex int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	persistence := ShardKVPersistence{
		Data:           kv.data,
		LatestRequests: kv.latestRequests,
		LatestConfig:   kv.latestConfig,
		ShardStatus:    kv.shardStatus,
	}
	e.Encode(persistence)

	data := w.Bytes()
	kvDebug("Saving snapshot. Size: %d bytes", kv, len(data))
	kv.persister.SaveSnapshot(data)

	// Compact raft log til index.
	kv.rf.CompactLog(logIndex)
}

func (kv *ShardKV) loadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	obj := ShardKVPersistence{}
	d.Decode(&obj)

	kv.data = obj.Data
	kv.latestRequests = obj.LatestRequests
	kv.latestConfig = obj.LatestConfig
	kv.shardStatus = obj.ShardStatus
	kvInfo("Loaded snapshot. %d bytes", kv, len(data))
}

func (kv *ShardKV) statusProcess() {
	timer := time.NewTicker(StatusUpdateInterval)

	for {
		select {
		case <-timer.C:
			kv.Lock()
			if kv.isDecommissioned {
				kv.Unlock()
				return
			} else {
				kvInfo("Shard states: %v", kv, kv.shardStatus)
				kv.Unlock()
			}
		}
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
	gob.Register(SendShardArgs{})
	gob.Register(SendShardReply{})
	gob.Register(ShardTransferOp{})

	kv := ShardKV{
		me:               me,
		maxraftstate:     maxraftstate,
		make_end:         make_end,
		gid:              gid,
		masters:          masters,
		persister:        persister,
		applyCh:          make(chan raft.ApplyMsg),
		requestHandlers:  make(map[int]chan raft.ApplyMsg),
		data:             make(map[int]map[string]string),
		latestRequests:   make(map[int]map[int64]int64),
		shardStatus:      make(map[int]ShardState),
		snapshotsEnabled: maxraftstate != -1,
	}

	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardStatus[i] = NotStored
	}

	if data := persister.ReadSnapshot(); kv.snapshotsEnabled && data != nil && len(data) > 0 {
		kv.loadSnapshot(data)
	}

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardmaster.NShards; i++ {
		if kv.shardStatus[i] == MigratingData {
			go kv.deleteShard(i, kv.latestConfig)
		} else if kv.shardStatus[i] == AwaitingData {
			go kv.createShardIfNeeded(i, kv.latestConfig)
		}
	}

	go kv.startConfigListener()
	go kv.startApplyProcess()
	go kv.statusProcess()

	kvInfo("Started group: %d, on node: %d", &kv, gid, me)

	return &kv
}
