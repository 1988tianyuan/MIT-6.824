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
	CANDIDATE_TIMEOUT_RANGE int64 = 400
	HEARTBEAT_TIMEOUT_RANGE int64 = 400
	HEARTBEAT_PERIOD              = time.Duration(100) * time.Millisecond
)

func (raft *Raft) Kill() {
	raft.isStart = false
}

func (raft *Raft) isFollower() bool {
	return raft.state == FOLLOWER
}

func (raft *Raft) isLeader() bool {
	return raft.state == LEADER
}

func (raft *Raft) isCandidate() bool {
	return raft.state == CANDIDATE
}

// need to be called in lock, and the term should be bigger than raft's CurrentTerm
func (raft *Raft) stepDown(term int)  {
	raft.CurTermAndVotedFor = CurTermAndVotedFor{CurrentTerm: term, VotedFor:-1}
	if !raft.isFollower() {
		log.Printf("StepDown==> term: %d, raft-id: %d, 收到最新的term: %d, 降职为FOLLOWER",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, term)
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
