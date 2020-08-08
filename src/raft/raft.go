package raft

import (
	"labrpc"
	"log"
)
// import "bytes"
// import "labgob"

func (raft *Raft) Start(command interface{}) (int, int, bool) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	return raft.internalStart(command, true)
}

func (raft *Raft) internalStart(command interface{}, commandValid bool) (int, int, bool) {
	var index int
	var term int
	if raft.IsLeader() {
		index = raft.LastLogIndex + 1
		term = raft.CurTermAndVotedFor.CurrentTerm
		raft.Logs = append(raft.Logs, ApplyMsg{CommandValid: commandValid, Term: term, CommandIndex: index, Command: command})
		raft.LastLogIndex = index
		raft.LastLogTerm = term
		go raft.writeRaftStatePersist()
	}
	return index, term, raft.IsLeader()
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	return ExtensionMake(peers, me, persister, applyCh, false)
}

func (raft *Raft) getLogEntry(index int) (bool, ApplyMsg) {
	offset := raft.getOffset(index)
	if offset >= 0 {
		entry := raft.Logs[offset]
		return true, entry
	} else {
		return false, ApplyMsg{}
	}
}

func (raft *Raft) GetState() (int, bool) {
	return raft.CurTermAndVotedFor.CurrentTerm, raft.IsLeader()
}

func (raft *Raft) getOffset(index int) int {
	if index > raft.LastIncludedIndex {
		offset := index - raft.LastIncludedIndex - 1
		if offset >= 0 {
			entry := raft.Logs[offset]
			if entry.CommandIndex != index {
				println("啊啊啊啊啊")
			}
		}
		return offset
	} else {
		return -1
	}
}

func ExtensionMake(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg, useDummyLog bool) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.Me = me
	raft.state = FOLLOWER		// init with FOLLOWER state
	raft.IsStart = true
	raft.applyCh = applyCh
	raft.persistCh = make(chan PersistStruct)
	raft.UseDummyLog = useDummyLog
	raft.persistSeq = 0
	raft.readRaftStatePersist()

	if len(raft.Logs) != 0 && raft.LastIncludedIndex + 1 != raft.Logs[0].CommandIndex {
		println("嘿嘿嘿")
	}
	if raft.persister.SnapshotSize() == 0 {
		raft.LastIncludedIndex = -1
		raft.LastIncludedTerm = -1
	}
	if len(raft.Logs) == 0 {
		raft.LastLogIndex = raft.LastIncludedIndex
		raft.LastLogTerm = raft.LastIncludedTerm
		raft.Logs = make([] ApplyMsg, 0)
		if raft.persister.SnapshotSize() == 0 {
			// init empty log for index=0
			raft.Logs = append(raft.Logs, ApplyMsg{CommandIndex: 0, CommandValid:false})
			raft.LastLogIndex++
		}
	}
	go raft.doFollowerJob()
	//go raft.loopPersistRaftState()
	return raft
}

func (raft *Raft) loopPersistRaftState() {
	for raft.IsStart {
		select {
		case persistStruct := <- raft.persistCh:
			//if persistStruct.persistSeq <= raft.persistSeq {
			//	continue
			//}
			raft.persistSeq = persistStruct.persistSeq
			if persistStruct.snapshot != nil {
				raft.persister.SaveStateAndSnapshot(persistStruct.raftState, persistStruct.snapshot)
			} else {
				raft.persister.SaveRaftState(persistStruct.raftState)
			}
			if raft.MaxStateSize > 0 && raft.persister.RaftStateSize() > (raft.MaxStateSize/2)*3 &&
				raft.LastIncludedIndex != raft.LastAppliedIndex{
				//raft.logCompact()
			}
		}
	}
}

/* return true means the specific index and term log has been successfully committed by raft */
func (raft *Raft) CheckCommittedIndexAndTerm(index int, term int) bool {
	raft.mu.RLock()
	defer raft.mu.RUnlock()
	notCompacted, entry := raft.getLogEntry(index)
	if notCompacted {
		return raft.LastAppliedIndex >= index && entry.Term == term
	} else {
		//TODO
		return raft.LastAppliedIndex >= index
	}
}

func (raft *Raft) IsApplied(index int, term int) bool {
	return raft.LastAppliedIndex >= index && raft.LastAppliedTerm >= term
}

func (raft *Raft) logCompact() {
	raft.mu.Lock()
	if raft.MaxStateSize <= 0 || raft.persister.RaftStateSize() < (raft.MaxStateSize/2)*3 {
		raft.mu.Unlock()
		return
	}
	log.Printf("CompactLog: raft-id: %d, lastAppliedIndex是: %d, lastIncludedIndex是: %d, lastAppliedTerm是: %d", raft.Me,
		raft.LastAppliedIndex, raft.LastIncludedIndex, raft.LastAppliedTerm)
	beginOffset := raft.getOffset(raft.LastAppliedIndex) + 1
	endOffset := raft.getOffset(raft.LastLogIndex)
	raft.LastIncludedIndex = raft.LastAppliedIndex
	raft.LastIncludedTerm = raft.LastAppliedTerm
	if raft.LastIncludedIndex < raft.LastLogIndex {
		raft.Logs = raft.Logs[beginOffset:endOffset + 1]
	} else if raft.LastIncludedIndex == raft.LastLogIndex {
		// all the logs have to be compacted
		raft.Logs = make([] ApplyMsg, 0)
	}
	raft.mu.Unlock()
	data := raft.serializeRaftState()
	if data != nil {
		persistSeq := currentTimeMillis()
		go raft.SaveStateAndSnapshotStruct(PersistStruct{data, nil, persistSeq})
	}
	if len(raft.Logs) != 0 && raft.LastLogIndex != raft.Logs[len(raft.Logs) - 1].CommandIndex {
		println("呵呵呵")
	}
	if len(raft.Logs) != 0 && raft.LastIncludedIndex + 1 != raft.Logs[0].CommandIndex {
		println("嗨嗨嗨")
	}
}