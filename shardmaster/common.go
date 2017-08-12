package shardmaster

import (
	"bytes"
	"fmt"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be unique and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) Clone() Config {
	groups := make(map[int][]string)
	for k, v := range c.Groups {
		groups[k] = v
	}

	return Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: groups,
	}
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	RequestId int64
	ClientId  int64
}

func (args JoinArgs) String() string {
	var buffer bytes.Buffer
	index := 0
	for k, v := range args.Servers {
		buffer.WriteString(fmt.Sprintf("%d -> %v", k, v))
		if index++; index < len(args.Servers) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

type LeaveArgs struct {
	GIDs      []int
	RequestId int64
	ClientId  int64
}

func (args LeaveArgs) String() string {
	return fmt.Sprintf("GIDs:%v", args.GIDs)
}

type MoveArgs struct {
	Shard     int
	GID       int
	RequestId int64
	ClientId  int64
}

func (args MoveArgs) String() string {
	return fmt.Sprintf("Shard:%d, GID:%d", args.Shard, args.GID)
}

type QueryArgs struct {
	Num       int // desired config number
	RequestId int64
	ClientId  int64
}

type RequestReply struct {
	WrongLeader bool
	Err         Err
	Config      Config // Optional
}
