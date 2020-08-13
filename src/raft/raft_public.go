package raft

/* the functions that exposed to other modules to call */

import (
	"fmt"
)

func (raft *Raft) LogCompact(snapshotCount int, afterCompact func()) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	// double check
	if (raft.LastAppliedIndex - raft.LastIncludedIndex) <= snapshotCount || raft.LastIncludedIndex == raft.LastAppliedIndex {
		return
	}
	PrintLog("CompactLog: raft-id: %d, lastAppliedIndex是: %d, lastIncludedIndex是: %d, lastAppliedTerm是: %d", raft.Me,
		raft.LastAppliedIndex, raft.LastIncludedIndex, raft.LastAppliedTerm)
	beginOffset := raft.getOffset(raft.LastAppliedIndex) + 1
	raft.LastIncludedIndex = raft.LastAppliedIndex
	raft.LastIncludedTerm = raft.LastAppliedTerm
	if raft.LastIncludedIndex < raft.LastLogIndex {
		raft.Logs = raft.Logs[beginOffset:]
	} else if raft.LastIncludedIndex == raft.LastLogIndex {
		// all the logs have to be compacted
		PrintLog("CompactLog: raft-id: %d, 结束, LastIncludedIndex:%d, LastLogIndex:%d, 切割完变成空的了", raft.Me,
			raft.LastIncludedIndex, raft.LastLogIndex)
		raft.Logs = make([] ApplyMsg, 0)
	}
	if len(raft.Logs) > 0 {
		_, entry := raft.getLogEntry(raft.LastIncludedIndex + 1)
		if entry.CommandIndex != raft.LastIncludedIndex + 1 {
			PrintLog("CompactLog: raft-id: %d, 结束后，lastAppliedIndex是: %d, " +
				"lastIncludedIndex是: %d, first entry index是: %d", raft.Me,
				raft.LastAppliedIndex, raft.LastIncludedIndex, entry.CommandIndex)
		}
	}
	afterCompact()
	PrintLog("CompactLog: raft-id: %d, 顺利切割完日志啦！, LastIncludedIndex:%d, LastLogIndex:%d, log的长度:%d", raft.Me,
		raft.LastIncludedIndex, raft.LastLogIndex, len(raft.Logs))
}

func (raft *Raft) ReplayRange() {
	lastIncludedIndex := raft.LastIncludedIndex
	lastAppliedIndex := raft.LastAppliedIndex
	if lastIncludedIndex < lastAppliedIndex {
		beginOffset := raft.getOffset(lastIncludedIndex + 1)
		endOffset := raft.getOffset(lastAppliedIndex)
		beginEntry := raft.Logs[beginOffset]
		if beginEntry.CommandIndex != lastIncludedIndex + 1 {
			err := fmt.Errorf("LastIncludedIndex is %d, LastAppliedIndex is %d, " +
				"beginEntry's index:%d is not the same with LastIncludedIndex+1, there must be some problem",
				raft.LastIncludedIndex, raft.LastAppliedIndex, beginEntry.CommandIndex)
			panic(err)
		}
		PrintLog("ReplayRange==> term: %d, raft-id: %d, 重放范围是:%d-%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, lastIncludedIndex + 1, lastAppliedIndex)
		entries := raft.Logs[beginOffset : endOffset + 1]
		for _, entry := range entries {
			entry.Type = REPLAY
			raft.applyCh <- entry
		}
	} else if lastIncludedIndex == lastAppliedIndex {
		PrintLog("ReplayRange==> term: %d, raft-id: %d, LastIncludedIndex:%d 和 LastAppliedIndex:%d 两者相等，无需重放",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, lastIncludedIndex, lastAppliedIndex)
	} else {
		err := fmt.Errorf("LastIncludedIndex is %d, LastAppliedIndex is %d, " +
			"LastIncludedIndex is bigger than LastAppliedIndex, there must be some problem",
			raft.LastIncludedIndex, raft.LastAppliedIndex)
		panic(err)
	}
}

func (raft *Raft) GetState() (int, bool) {
	return raft.CurTermAndVotedFor.CurrentTerm, raft.IsLeader()
}

func (raft *Raft) IsStart() bool {
	return DOWN != raft.state
}
