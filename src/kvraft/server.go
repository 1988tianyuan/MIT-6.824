package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"raft"
	"strconv"
	"time"
)

func (kv *KVServer) startRaft(op Operation, key string, value string, reply *CommonReply, isRead bool,
	clientId int64, requestSeq int64) {
	rf := kv.rf
	curIndex, curTerm, isLeader := rf.Start(genCommand(op, key, value, clientId, requestSeq))
	kv.mu.Lock()
	notiCh := make(chan ApplyNoti)
	kv.ApplyNotifyChMap[curIndex] = notiCh
	kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
	} else {
		select {
		case <- time.After(time.Duration(200) * time.Millisecond):
			PrintLog("StartRaft: raft执行超时，直接返回")
			reply.Err = "failed to execute request, please try again."
			break
		case applyNoti := <- notiCh:
			if !rf.IsLeader() {
				reply.Err = "failed to execute request, please try again."
				reply.WrongLeader = true
				break
			}
			if isRead {
				if applyNoti.key == key {
					reply.Content = applyNoti.value
				} else {
					reply.Err = "failed to execute read request, please try again."
				}
			}
			break
		}
		kv.mu.Lock()
		delete(kv.ApplyNotifyChMap, curIndex)
		kv.mu.Unlock()
		reply.IndexAndTerm = strconv.Itoa(curIndex) + ":" + strconv.Itoa(curTerm) + ":" + strconv.Itoa(rf.Me)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *CommonReply) {
	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	PrintLog("PutAppend: 收到PutAppend, args是:%v, 当前的server是:%d", args, kv.rf.Me)
	op := args.Op
	if op != PUT && op != APPEND {
		reply.Err = "Wrong op, should be one of these op: Put | Append."
	} else {
		kv.mu.Lock()
		if kv.ClientReqSeqMap[args.ClientId] >= args.RequestSeq {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
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
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(KVCommand{})
	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	kv.readPersistedStore()
	kv.rf = raft.ExtensionMake(servers, me, persister, kv.applyCh, true)
	kv.snapshotCount = 15
	if kv.KvMap == nil {
		kv.KvMap = make(map[string]string)
	}
	if kv.ClientReqSeqMap == nil {
		kv.ClientReqSeqMap = make(map[int64]int64)
	}
	if kv.ApplyNotifyChMap == nil {
		kv.ApplyNotifyChMap = make(map[int]chan ApplyNoti)
	}
	kv.rf.OnRaftLeaderSelected = func (raft *raft.Raft) {
		kvMap := &kv.KvMap
		PrintLog("OnRaftLeaderSelected==> term: %d, raft-id: %d, 当前的快照是: %v",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, *kvMap)
		PrintLog("OnRaftLeaderSelected==> term: %d, raft-id: %d, 当前的raft状态是: %v",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, raft)
	}
	go kv.loopApply()
	kv.replay()
	return kv
}

func (kv *KVServer) replay() {
	if kv.rf.LastIncludedIndex < kv.rf.LastAppliedIndex {
		kv.mu.RLock()
		// need to replay: from LastIncludedIndex+1 to LastAppliedIndex
		PrintLog("Replay==> term: %d, raft-id: %d, 开始REPLAY, 现在的kvMap是: %v",
			kv.rf.CurTermAndVotedFor.CurrentTerm, kv.rf.Me, kv.KvMap)
		kv.mu.RUnlock()
		kv.rf.ReplayRange()
		kv.mu.RLock()
		PrintLog("Replay==> term: %d, raft-id: %d, 结束REPLAY, 现在的kvMap是: %v",
			kv.rf.CurTermAndVotedFor.CurrentTerm, kv.rf.Me, kv.KvMap)
		kv.mu.RUnlock()
	}
}

func (kv *KVServer) loopApply() {
	for kv.IsRunning() {
		select {
		case apply := <- kv.applyCh:
			if apply.Type == raft.REPLAY {
				kv.mu.Lock()
				if apply.CommandValid {
					kv.applyKVStore(apply.Command.(KVCommand), apply.CommandIndex)
				}
				kv.mu.Unlock()
			} else if apply.Type == raft.APPEND_ENTRY && kv.rf.LastAppliedIndex < apply.CommandIndex && apply.CommandValid {
				kv.mu.Lock()
				if apply.CommandValid {
					kv.applyKVStore(apply.Command.(KVCommand), apply.CommandIndex)
				}
				kv.rf.LastAppliedIndex = apply.CommandIndex
				kv.rf.LastAppliedTerm = apply.Term
				PrintLog("LoopApply==> term: %d, raft-id: %d, 将index:%d 提交到状态机",
					kv.rf.CurTermAndVotedFor.CurrentTerm, kv.rf.Me, apply.CommandIndex)
				kv.mu.Unlock()
				go kv.checkSnapshot()
			} else if apply.Type == raft.INSTALL_SNAPSHOT {
				kv.mu.Lock()
				buffer := bytes.NewBuffer(apply.SnapshotData)
				decoder := labgob.NewDecoder(buffer)
				kv.readPersistedKvMap(decoder)
				kv.readReqSeqMap(decoder)
				kv.rf.WriteRaftStateAndSnapshotPersist(apply.SnapshotData)
				PrintLog("LoopApply==> term: %d, raft-id: %d, 收到INSTALL_SNAPSHOT, 最后kvMap是: %v",
					kv.rf.CurTermAndVotedFor.CurrentTerm, kv.rf.Me, kv.KvMap)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) checkSnapshot() {
	if kv.maxraftstate > 0 && (kv.rf.LastAppliedIndex - kv.rf.LastIncludedIndex) > kv.snapshotCount &&
		kv.rf.LastIncludedIndex != kv.rf.LastAppliedIndex {
		kv.rf.LogCompact(kv.snapshotCount, func() {
			kv.persistStore()
		})
	}
}

func (kv *KVServer) applyKVStore(command KVCommand, index int) {
	applyNoti := ApplyNoti{key: command.Key, value: kv.KvMap[command.Key]}
	applyNotiCh := kv.ApplyNotifyChMap[index]
	op := command.Operation
	if op != GET {
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
	if applyNotiCh != nil {
		applyNotiCh <- applyNoti
	}
}
