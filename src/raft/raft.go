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
	index := raft.Logs[len(raft.Logs) - 1].CommandIndex + 1
	term := raft.CurTermAndVotedFor.CurrentTerm
	if raft.IsLeader() {
		raft.Logs = append(raft.Logs, ApplyMsg{CommandValid: commandValid, Term: term, CommandIndex: index, Command: command})
		raft.LastLogIndex = index
		raft.LastLogTerm = term
		go raft.persistState()
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
	raft.IsStart = true
	raft.readPersist(persister.ReadRaftState())
	raft.applyCh = applyCh
	raft.UseDummyLog = useDummyLog
	if len(raft.Logs) == 0 {
		raft.Logs = make([] ApplyMsg, 0)
		raft.Logs = append(raft.Logs, ApplyMsg{CommandIndex: 0, CommandValid:false}) // init empty log for index=0
		raft.LastLogIndex = len(raft.Logs) - 1
		raft.LastLogTerm = -1
	}
	go raft.doFollowerJob()
	return raft
}

/* return true means the specific index and term log has been successfully committed by raft */
func (raft *Raft) CheckCommittedIndexAndTerm(index int, term int) bool {
	if raft.CommitIndex >= index && raft.Logs[index].Term == term {
		return true
	}
	return false
}