package raft

import (
	"log"
	"time"
)

func (raft *Raft) doCandidateJob() {
	for raft.isStart {
		raft.mu.Lock()
		raft.state = CANDIDATE
		currentTerm := raft.CurTermAndVotedFor.CurrentTerm
		raft.CurTermAndVotedFor =
			CurTermAndVotedFor{CurrentTerm: currentTerm + 1, VotedFor:raft.me} // increment term and vote for self
		timeout := makeRandomTimeout(300, CANDIDATE_TIMEOUT_RANGE)             // random election timeout
		go raft.persist()
		go raft.beginLeaderElection(timeout)
		raft.mu.Unlock()
		time.Sleep(timeout)
		if raft.isCandidate() {		// election timeout
			log.Printf("DoCandidateJob==> term: %d, raft-id: %d, 选举超时, 重新开始选举",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me)
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
		replyTerm := reply.Term
		if replyTerm > raft.CurTermAndVotedFor.CurrentTerm {
			raft.mu.Lock()
			defer raft.mu.Unlock()
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
			CandidateId:raft.me,
			LastLogTerm:raft.LastLogTerm,
			LastLogIndex:raft.LastLogIndex}
		votes := 1
		for server := range raft.peers {
			if server == raft.me {
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
					log.Printf("BeginLeaderElection==> term: %d, raft-id: %d, 从raft:%d 处获得1票",
						raft.CurTermAndVotedFor.CurrentTerm, raft.me, reply.Server)
					votes++
				}
				if votes >= threshold && raft.isCandidate() {
					raft.mu.Lock()
					raft.changeToLeader(votes)
					raft.mu.Unlock()
					return
				}
			case <-timer:
				return
			}
		}
	}
}
