package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "github.com/sunhay/mit-6.824-2017/labrpc"
import "crypto/rand"
import "math/big"
import "github.com/sunhay/mit-6.824-2017/shardmaster"
import (
	"strconv"
	"time"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	id       int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := Clerk{
		sm:       shardmaster.MakeClerk(masters),
		make_end: make_end,
		id:       nrand(),
	}
	return &ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.id,
		RequestId: nrand(),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply RequestReply
				if ok := srv.Call("ShardKV.Get", &args, &reply); ok {
					if reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
						return reply.Value
					} else if reply.Err == ErrWrongGroup { // Use the received group id for our next request
						si = 0
						gid, _ = strconv.Atoi(reply.Value)
						servers, ok = ck.config.Groups[gid]
						if !ok {
							break
						}
					} else if reply.Err == ErrMovingShard {
						si-- // Retry request?
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.id,
		RequestId: nrand(),
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply RequestReply
				if ok := srv.Call("ShardKV.PutAppend", &args, &reply); ok {
					if reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
						return
					} else if reply.Err == ErrWrongGroup { // Use the received group id for our next request
						si = 0
						gid, _ = strconv.Atoi(reply.Value)
						servers, ok = ck.config.Groups[gid]
						if !ok {
							break
						}
					} else if reply.Err == ErrMovingShard {
						si-- // Retry request?
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
