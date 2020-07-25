package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"time"
)

const Debug = 0

func (kv *KVServer) startRaft(op Operation, key string, value string, reply *CommonReply, isRead bool,
	clientId int64, requestSeq int64) {
	rf := kv.rf
	curIndex, curTerm, isLeader := rf.Start(genCommand(op, key, value, clientId, requestSeq))
	if !isLeader {
		reply.WrongLeader = true
	} else {
		for !kv.isApplied(curIndex, curTerm) {
			if !rf.IsLeader() {
				reply.WrongLeader = true
				return
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
		if isRead {
			reply.Content = kv.KvMap[key]
		}
		if !rf.CheckCommittedIndexAndTerm(curIndex, curTerm) {
			reply.Err = "failed to execute request, please try again."
		}
	}
}

func (kv *KVServer) isApplied(index int, term int) bool {
	return kv.LastAppliedIndex >= index && kv.LastAppliedTerm >= term
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *CommonReply) {
	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	log.Printf("PutAppend: 收到PutAppend, args是:%v, 当前的server是:%d", args, kv.rf.Me)
	op := args.Op
	if op != PUT && op != APPEND {
		reply.Err = "Wrong op, should be one of these op: Put | Append."
	} else {
		if kv.ClientReqSeqMap[args.ClientId] >= args.RequestSeq {
			return
		}
		value := args.Value
		kv.startRaft(op, args.Key, value, reply, false, args.ClientId, args.RequestSeq)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *CommonReply) {
	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	kv.startRaft(GET, args.Key, "", reply, true, 0, 0)
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
	labgob.Register(KVCommand{})

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
	if kv.ClientReqSeqMap == nil {
		kv.ClientReqSeqMap = make(map[int64]int64)
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
				kv.applyKVStore(apply.Command.(KVCommand))
			}
			kv.LastAppliedIndex = apply.CommandIndex
			kv.LastAppliedTerm = apply.Term
			kv.persistStore()
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyKVStore(command KVCommand) {
	op := command.Operation
	if op == GET {
		return
	}
	key := command.Key
	value := command.Value
	clientId := command.ClientId
	requestSeq := command.RequestSeq
	reqSeqMap := kv.ClientReqSeqMap
	if reqSeqMap[clientId] < requestSeq {
		reqSeqMap[clientId] = requestSeq
	} else {
		return
	}
	switch op {
	case PUT:
		kv.KvMap[key] = value
		break
	case APPEND:
		oldValue := kv.KvMap[key]
		kv.KvMap[key] = oldValue + value
		break
	}
}
