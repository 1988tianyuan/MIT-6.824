package raft

import (
	"labrpc"
	"log"
)

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
		raft.Logs = append(raft.Logs, ApplyMsg{CommandValid: commandValid, Term: term, CommandIndex: index, Command: command,
			Type: APPEND_ENTRY})
		raft.LastLogIndex = index
		raft.LastLogTerm = term
		go raft.writeRaftStatePersist()
	}
	return index, term, raft.IsLeader()
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	return ExtensionMake(peers, me, persister, applyCh, false)
}

func ExtensionMake(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg, useDummyLog bool) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.Me = me
	raft.state = FOLLOWER		// init with FOLLOWER state
	raft.applyCh = applyCh
	raft.UseDummyLog = useDummyLog
	raft.readRaftStatePersist()
	if raft.persister.SnapshotSize() == 0 {
		raft.LastIncludedIndex = -1
		raft.LastIncludedTerm = -1
	}
	PrintLog("ExtensionMake: raft-id: %d, 这时候log的长度是:%d, raft的状态是:%d", raft.Me,
		raft.LastIncludedIndex, len(raft.Logs), raft)
	if len(raft.Logs) == 0 {
		raft.LastLogIndex = raft.LastIncludedIndex
		raft.LastLogTerm = raft.LastIncludedTerm
		raft.Logs = make([] ApplyMsg, 0)
		if raft.persister.SnapshotSize() == 0 {
			// init empty log for index=0
			raft.LastAppliedIndex = raft.LastIncludedIndex
			raft.LastAppliedTerm = raft.LastIncludedTerm
			raft.Logs = append(raft.Logs, ApplyMsg{CommandIndex: 0, CommandValid:false, Type: APPEND_ENTRY})
			raft.LastLogIndex++
		}
	}
	go raft.doFollowerJob()
	return raft
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

func (raft *Raft) getOffset(index int) int {
	if index > raft.LastIncludedIndex {
		offset := index - raft.LastIncludedIndex - 1
		if offset >= 0 {
			entry := raft.Logs[offset]
			if entry.CommandIndex != index {
				println("啊啊啊啊啊")
				panic("aaaaa")
				log.Fatal("hhahahah")
			}
		}
		return offset
	} else {
		return -1
	}
}

func (raft *Raft) checkApply() {
	if raft.LastAppliedIndex < raft.CommitIndex {
		beginApplyIndex := raft.LastAppliedIndex + 1
		for beginApplyIndex <= raft.CommitIndex {
			_, logEntry := raft.getLogEntry(beginApplyIndex)
			raft.applyCh <- logEntry
			beginApplyIndex++
		}
	}
}