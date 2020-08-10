package raftkv

import (
	cryptoRand "crypto/rand"
	"math/big"
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
	RequestSeq  int64
	ClientId	int64
}

type GetArgs struct {
	Key string
}

type CommonReply struct {
	WrongLeader bool
	Err         Err
	Content     interface{}
	IndexAndTerm  string
}

func genCommand(op Operation, key string, value string, clientId int64, requestSeq int64) KVCommand {
	return KVCommand{op, key, value, clientId, requestSeq}
}

type KVCommand struct {
	Operation Operation
	Key 	  string
	Value     string
	ClientId  int64
	RequestSeq int64
}

type KVServer struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	snapshotCount int
	// public properties to persist
	KvMap        map[string]string
	ClientReqSeqMap map[int64]int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := cryptoRand.Int(cryptoRand.Reader, max)
	x := bigx.Int64()
	return x
}

func (kv *KVServer) IsRunning() bool {
	return kv.rf.IsStart()
}