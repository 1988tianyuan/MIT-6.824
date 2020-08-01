package raft

import "labrpc"
// import "bytes"
// import "labgob"

func (raft *Raft) Start(command interface{}) (int, int, bool) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	return raft.internalStart(command, true)
}

func (raft *Raft) internalStart(command interface{}, commandValid bool) (int, int, bool) {
	index := len(raft.Logs) + raft.LastIncludedIndex + 1
	term := raft.CurTermAndVotedFor.CurrentTerm
	if raft.IsLeader() {
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
		return true, raft.Logs[offset]
	} else {
		return false, ApplyMsg{}
	}
}

func (raft *Raft) GetState() (int, bool) {
	return raft.CurTermAndVotedFor.CurrentTerm, raft.IsLeader()
}

func (raft *Raft) getOffset(index int) int {
	if index > raft.LastIncludedIndex {
		return index - raft.LastIncludedIndex - 1
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
	raft.UseDummyLog = useDummyLog
	raft.readRaftStatePersist()

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
	return raft
}

/* return true means the specific index and term log has been successfully committed by raft */
func (raft *Raft) CheckCommittedIndexAndTerm(index int, term int) bool {
	notCompacted, entry := raft.getLogEntry(index)
	if notCompacted {
		return raft.CommitIndex >= index && entry.Term == term
	} else {
		//TODO
		return raft.CommitIndex >= index
	}
}

func (raft *Raft) CompactLog(lastAppliedIndex int, lastAppliedTerm int) {
	beginOffset := raft.getOffset(lastAppliedIndex) + 1
	endOffset := raft.getOffset(raft.LastLogIndex)
	raft.LastIncludedIndex = lastAppliedIndex
	raft.LastIncludedTerm = lastAppliedTerm
	if raft.LastIncludedIndex < raft.LastLogIndex {
		raft.Logs = raft.Logs[beginOffset:endOffset + 1]
	} else if raft.LastIncludedIndex == raft.LastLogIndex {
		// all the logs have to be compacted
		raft.Logs = make([] ApplyMsg, 0)
	}
}