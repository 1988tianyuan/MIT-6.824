package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
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
		for !kv.rf.IsApplied(curIndex, curTerm) {
			if !rf.IsLeader() {
				reply.WrongLeader = true
				return
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
		if isRead {
			kv.mu.RLock()
			reply.Content = kv.KvMap[key]
			kv.mu.RUnlock()
		}
		if !rf.IsLeader() || !rf.CheckCommittedIndexAndTerm(curIndex, curTerm) {
			reply.Err = "failed to execute request, please try again."
		}
		reply.IndexAndTerm = strconv.Itoa(curIndex) + ":" + strconv.Itoa(curTerm)
	}
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

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(KVCommand{})

	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.readPersistedStore()
	kv.rf = raft.ExtensionMake(servers, me, persister, kv.applyCh, true)
	kv.rf.MaxStateSize = maxraftstate
	if kv.KvMap == nil {
		kv.KvMap = make(map[string]string)
	}
	if kv.ClientReqSeqMap == nil {
		kv.ClientReqSeqMap = make(map[int64]int64)
	}
	kv.rf.OnRaftLeaderSelected = func (raft *raft.Raft) {
		kvMap := &kv.KvMap
		log.Printf("OnRaftLeaderSelected==> term: %d, raft-id: %d, 当前的快照是: %v",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, *kvMap)
		log.Printf("OnRaftLeaderSelected==> term: %d, raft-id: %d, 当前的raft状态是: %v",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, raft)
	}
	kv.rf.OnReceiveSnapshot = func (snapshotBytes []byte, lastIncludedIndex int, lastIncludedTerm int,
		raftHandleFunc func(int, int)) {
		kv.mu.Lock()
		buffer := bytes.NewBuffer(snapshotBytes)
		decoder := labgob.NewDecoder(buffer)
		kv.rf.RaftLock()
		raftHandleFunc(lastIncludedIndex, lastIncludedTerm)
		kv.rf.RaftUnlock()
		kv.readPersistedKvMap(decoder)
		kv.readReqSeqMap(decoder)
		kv.mu.Unlock()
		kv.rf.WriteRaftStateAndSnapshotPersist(snapshotBytes)
	}

	go kv.loopApply()
	return kv
}

func (kv *KVServer) loopApply() {
	for kv.IsRunning() {
		select {
		case apply := <- kv.applyCh:
			if kv.rf.LastAppliedIndex < apply.CommandIndex {
				//kv.rf.RaftLock()
				kv.rf.LastAppliedIndex = apply.CommandIndex
				kv.rf.LastAppliedTerm = apply.Term
				//kv.rf.RaftUnlock()
				kv.mu.Lock()
				if apply.CommandValid {
					kv.applyKVStore(apply.Command.(KVCommand))
				}
				kv.mu.Unlock()
				kv.persistStore()
			}
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
