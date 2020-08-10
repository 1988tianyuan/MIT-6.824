package raft

import (
	"log"
	"math/rand"
	"time"
)


const (
	LEADER                  State = "LEADER"
	CANDIDATE               State = "CANDIDATE"
	FOLLOWER                State = "FOLLOWER"
	DOWN                	State = "DOWN"
	CANDIDATE_TIMEOUT_RANGE int64 = 400
	HEARTBEAT_TIMEOUT_RANGE int64 = 400
	HEARTBEAT_PERIOD              = time.Duration(100) * time.Millisecond
	APPEND_ENTRY 			MsgType = "APPEND_ENTRY"
	INSTALL_SNAPSHOT 		MsgType	= "INSTALL_SNAPSHOT"
	REPLAY					MsgType = "REPLAY"
)

var DEBUG = false

func (raft *Raft) RaftLock() {
	raft.mu.Lock()
}

func (raft *Raft) RaftUnlock() {
	raft.mu.Unlock()
}

func (raft *Raft) Kill() {
	raft.state = DOWN
}

func (raft *Raft) isFollower() bool {
	return raft.state == FOLLOWER
}

func (raft *Raft) IsLeader() bool {
	return raft.state == LEADER
}

func (raft *Raft) isCandidate() bool {
	return raft.state == CANDIDATE
}

// need to be called in lock, and the term should be bigger than raft's CurrentTerm
func (raft *Raft) stepDown(term int)  {
	raft.CurTermAndVotedFor = CurTermAndVotedFor{CurrentTerm: term, VotedFor:-1}
	if !raft.isFollower() {
		PrintLog("StepDown==> term: %d, raft-id: %d, 收到最新的term: %d, 降职为FOLLOWER",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, term)
		raft.state = FOLLOWER
		go raft.doFollowerJob()
	}
}

func makeRandomTimeout(start int64, ran int64) time.Duration {
	return time.Duration(rand.Int63n(ran) + start) * time.Millisecond
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func PrintLog(format string, args ...interface{}) {
	if DEBUG {
		log.Printf(format, args)
	}
}
