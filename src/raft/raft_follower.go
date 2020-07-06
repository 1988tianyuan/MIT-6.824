package raft

import (
	"log"
	"time"
)

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
					raft.CurTermAndVotedFor.CurrentTerm, raft.me)
				go raft.doCandidateJob()
				break
			}
		}
	}
}
