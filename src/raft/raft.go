package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "labrpc"
// import "bytes"
// import "labgob"

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (raft *Raft) Start(command interface{}) (int, int, bool) {
	index := len(raft.logs)
	term := raft.curTermAndVotedFor.currentTerm
	if raft.isLeader() {
		raft.logs = append(raft.logs, ApplyMsg{Term: term, CommandIndex: index, Command: command})
		raft.lastLogIndex = index
		raft.lastLogTerm = term
		go raft.syncLogsToFollowers(makeRandomTimeout(600, 150))
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
	raft.logs = make([] ApplyMsg, 0)
	raft.logs = append(raft.logs, ApplyMsg{})		// init empty log for index=0

	go raft.doFollowerJob()

	//Your initialization code here (2A, 2B, 2C).//todo
	return raft
}