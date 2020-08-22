package raft

import (
	"time"
)

func (raft *Raft) doCandidateJob() {
	for raft.IsStart() && !raft.IsLeader() {
		raft.mu.Lock()
	PrintLog("doCandidateJob:raft:%d获取了锁", raft.Me)
		raft.state = CANDIDATE
		currentTerm := raft.CurTermAndVotedFor.CurrentTerm
		raft.CurTermAndVotedFor =
			CurTermAndVotedFor{CurrentTerm: currentTerm + 1, VotedFor:raft.Me} // increment term and vote for self
		timeout := makeRandomTimeout(300, CANDIDATE_TIMEOUT_RANGE)             // random election timeout
		raft.writeRaftStatePersist()
		go raft.beginLeaderElection(timeout)
		raft.mu.Unlock()
		time.Sleep(timeout)
		if raft.isCandidate() {		// election timeout
			PrintLog("DoCandidateJob==> term: %d, raft-id: %d, 选举超时, 重新开始选举",
				raft.CurTermAndVotedFor.CurrentTerm, raft.Me)
		} else {
			break
		}
	}
}

func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyChan chan RequestVoteReply) {
	reply := RequestVoteReply{}
	reply.HasStepDown = false
	reply.Server = server
	ok := raft.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		raft.mu.Lock()
	PrintLog("sendRequestVote:raft:%d获取了锁", raft.Me)
		defer raft.mu.Unlock()
		replyTerm := reply.Term
		if replyTerm > raft.CurTermAndVotedFor.CurrentTerm {
			raft.stepDown(replyTerm)
			reply.HasStepDown = true
		}
		replyChan <- reply
	}
}

func (raft *Raft) beginLeaderElection(timeout time.Duration) {
	if raft.isCandidate() {
		replyChan := make(chan RequestVoteReply, len(raft.peers) - 1)  // channel for receive async vote request
		args := &RequestVoteArgs{
			Term:raft.CurTermAndVotedFor.CurrentTerm,
			CandidateId:raft.Me,
			LastLogTerm:raft.LastLogTerm,
			LastLogIndex:raft.LastLogIndex}
		votes := 1
		for server := range raft.peers {
			if server == raft.Me {
				continue
			}
			go raft.sendRequestVote(server, args, replyChan)
		}
		timer := time.After(timeout)
		threshold := len(raft.peers)/2 + 1
		for raft.isCandidate() {
			select {
			case reply := <- replyChan:
				if reply.HasStepDown {
					return
				}
				if reply.VoteGranted {
					PrintLog("BeginLeaderElection==> term: %d, raft-id: %d, 从raft:%d 处获得1票",
						raft.CurTermAndVotedFor.CurrentTerm, raft.Me, reply.Server)
					votes++
				}
				raft.mu.Lock()
	PrintLog("for raft.isCandidate():raft:%d获取了锁", raft.Me)
				if votes >= threshold && raft.isCandidate() {
					raft.changeToLeader(votes)
					raft.mu.Unlock()
					PrintLog("BeginLeaderElection==> term: %d, raft-id: %d, 我是leader啦！",
						raft.CurTermAndVotedFor.CurrentTerm, raft.Me)
					return
				}
				raft.mu.Unlock()
			case <-timer:
				return
			}
		}
	}
}
