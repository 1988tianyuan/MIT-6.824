package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strings"
	"time"
)

const Debug = 0

func (kv *KVServer) startRaft(op Operation, key string, value string, reply *CommonReply) {
	rf := kv.rf
	if !rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	curIndex, curTerm, isLeader := rf.Start(genCommand(op, key, value))
	if !isLeader {
		reply.WrongLeader = true
	} else {
		timeout := time.Now().Add(1 * time.Second)
		for !rf.CheckCommittedIndexAndTerm(curIndex, curTerm) {
			time.Sleep(time.Duration(100) * time.Millisecond)
			if time.Now().After(timeout) {
				break
			}
		}
		if !rf.CheckCommittedIndexAndTerm(curIndex, curTerm) {
			reply.Err = "failed to execute request, please try again."
		} else {
			reply.Content = kv.KvMap[key]
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *CommonReply) {
	log.Printf("PutAppend: 收到PutAppend, args是:%v, 当前的server是:%d", args, kv.rf.Me)
	op := args.Op
	if op != PUT && op != APPEND {
		reply.Err = "Wrong op, should be one of these op: Put | Append."
	} else {
		kv.tryPutOrAppend(op, args, reply)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *CommonReply) {
	kv.startRaft(GET, args.Key, "", reply)
}

func (kv *KVServer) tryPutOrAppend(op Operation, args *PutAppendArgs, reply *CommonReply)  {
	value := args.Value
	kv.startRaft(op, args.Key, value, reply)
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
	case string(PUT):
		kv.KvMap[key] = value
		break
	case string(APPEND):
		oldValue := kv.KvMap[key]
		if !strings.Contains(oldValue, value) {
			kv.KvMap[key] = oldValue + value
		}
		break
	}
}
