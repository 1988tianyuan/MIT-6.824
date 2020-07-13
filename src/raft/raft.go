package raft

import "labrpc"
// import "bytes"
// import "labgob"

func (raft *Raft) Start(command interface{}) (int, int, bool) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	index := raft.Logs[len(raft.Logs) - 1].CommandIndex + 1
	term := raft.CurTermAndVotedFor.CurrentTerm
	if raft.isLeader() {
		raft.Logs = append(raft.Logs, ApplyMsg{CommandValid: true, Term: term, CommandIndex: index, Command: command})
		raft.LastLogIndex = index
		raft.LastLogTerm = term
		go raft.persistState()
	}
	return index, term, raft.isLeader()
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.me = me
	raft.state = FOLLOWER		// init with FOLLOWER state
	raft.isStart = true
	raft.readPersist(persister.ReadRaftState())
	raft.applyCh = applyCh
	if len(raft.Logs) == 0 {
		raft.Logs = make([] ApplyMsg, 0)
		raft.Logs = append(raft.Logs, ApplyMsg{CommandIndex: 0, CommandValid:false}) // init empty log for index=0
	}
	go raft.doFollowerJob()
	return raft
}