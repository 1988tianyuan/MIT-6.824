package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strings"
	"sync"
	"time"
)

const Debug = 0


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
	AppliedIndex int
	AppliedTerm  int
	persister    *raft.Persister
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	rf := kv.rf
	reply.CurrentServer = rf.Me
	if !rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	key := args.Key
	curIndex, cruTerm, isLeader := rf.Start(genCommand("Get", key, ""))
	if !isLeader {
		reply.WrongLeader = true
	} else {
		for kv.AppliedIndex < curIndex || kv.AppliedTerm < cruTerm {
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
	reply.Value = kv.KvMap[key]
}

func (kv *KVServer) tryKVStore(op string, args *PutAppendArgs, reply *PutAppendReply)  {
	key := args.Key
	value := args.Value
	rf := kv.rf
	curIndex, cruTerm, isLeader := rf.Start(genCommand(op, key, value))
	if !isLeader {
		reply.WrongLeader = true
		reply.LeaderIndex = rf.LeaderId
	} else {
		for kv.AppliedIndex < curIndex || kv.AppliedTerm < cruTerm {
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
		if !rf.CheckCommittedIndexAndTerm(curIndex, cruTerm) {
			reply.Err = "failed to update value, please try again."
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.LeaderIndex = kv.rf.LeaderId
	log.Printf("PutAppend: 收到PutAppend, args是:%v, 当前的server是:%d", args, kv.rf.Me)
	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	op := args.Op
	if op != "Put" && op != "Append" {
		reply.Err = "Wrong op, should be one of these op: Put | Append."
	} else {
		kv.tryKVStore(op, args, reply)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.readPersistedStore()
	kv.rf = raft.ExtensionMake(servers, me, persister, kv.applyCh, true)
	if kv.KvMap == nil {
		kv.KvMap = make(map[string]string)
	}
	go kv.loopApply()

	return kv
}

func (kv *KVServer) loopApply() {
	for kv.rf.IsStart {
		select {
		case apply := <- kv.applyCh:
			kv.mu.Lock()
			kv.updateAppliedInfo(apply)
			if apply.CommandValid {
				kv.applyKVStore(apply.Command.(string))
			}
			kv.persistStore()
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyKVStore(command string) {
	s := strings.Split(command, ",")
	op := s[0]
	key := s[1]
	value := s[2]
	switch op {
	case "Put":
		kv.KvMap[key] = value
		break
	case "Append":
		oldValue := kv.KvMap[key]
		if !strings.Contains(oldValue, value) {
			kv.KvMap[key] = oldValue + value
		}
		break
	}
}

func (kv *KVServer) updateAppliedInfo(apply raft.ApplyMsg)  {
	kv.AppliedIndex = apply.CommandIndex
	kv.AppliedTerm = apply.Term
}
