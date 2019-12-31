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

import (
	"log"
	"time"
)
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
		raft.syncLogsToFollowers()
	}
	return index, term, raft.isLeader()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.me = me
	raft.state = FOLLOWER		// init with FOLLOWER state
	raft.isStart = true
	raft.readPersist(persister.ReadRaftState())
	raft.applyCh = applyCh
	raft.logs = make([] ApplyMsg, 0)

	go raft.doFollowerJob()

	//Your initialization code here (2A, 2B, 2C).//todo
	return raft
}

/*
	just for follower, if heartbeat from leader is timeout, begin leader election
*/
func (raft *Raft) doFollowerJob() {
	raft.lastHeartBeatTime = currentTimeMillis()
	for raft.isStart {
		timeout := makeRandomTimeout(150, 150)
		time.Sleep(makeRandomTimeout(100, 100))
		if raft.isFollower() {
			current := currentTimeMillis()
			// leader heartbeat expired, change state to CANDIDATE and begin leader election
			if current > (raft.lastHeartBeatTime + timeout.Nanoseconds()/1000000) {
				log.Printf("DoFollowerJob==> term: %d, raft-id: %d, FOLLOWER等待超时，转换为CANDIDATE",
				raft.curTermAndVotedFor.currentTerm, raft.me)
				go raft.doCandidateJob()
				break
			}
		}
	}
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (raft *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (raft *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}