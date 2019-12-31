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

// need to be called in lock, and the term should be bigger than raft's currentTerm
func (raft *Raft) stepDown(term int)  {
	raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm:term, votedFor:-1}
	if !raft.isFollower() {
		raft.state = FOLLOWER
		go raft.doFollowerJob()
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func makeRandomTimeout(start int64, ran int64) time.Duration {
	return time.Duration(rand.Int63n(ran) + start) * time.Millisecond
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func (raft *Raft)printLog(method string, format string, v ...interface{})  {
	format = method + "==> term: %d, raft-id: %d, " + format
	log.Printf(format, raft.curTermAndVotedFor.currentTerm, raft.me, v)
}
