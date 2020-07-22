package raftkv

import (
	"raft"
	"sync"
)

const (
	PUT    Operation = "Put"
	APPEND Operation = "Append"
	GET    Operation = "Get"
)

type Operation string

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    Operation // "Put" or "Append"
}

type GetArgs struct {
	Key string
}

type CommonReply struct {
	WrongLeader bool
	Err         Err
	Content     interface{}
}

func genCommand(op Operation, key string, value string) string {
	return string(op) + "," + key + "," + value
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	KvMap        map[string]string
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
}