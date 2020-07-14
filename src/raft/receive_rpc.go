package raft

import (
	"log"
)

func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	recvTerm := args.Term
	candidateId := args.CandidateId
	log.Printf("RequestVote==> term: %d, raft-id: %d, 当前votedFor是%v, 给raft-id:%d投票，它的term是:%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.me, raft.CurTermAndVotedFor.VotedFor, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = raft.CurTermAndVotedFor.CurrentTerm

	// received term is smaller, reject this request and send back CurrentTerm
	if recvTerm < raft.CurTermAndVotedFor.CurrentTerm {
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d投反对票, 它的term是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, args.CandidateId, args.Term)
		return
	}
	// received is bigger, step to FOLLOWER
	if recvTerm > raft.CurTermAndVotedFor.CurrentTerm {
		raft.stepDown(recvTerm)
	}
	if raft.shouldGrant(args) {
		// refresh the follower's election timeout
		raft.lastHeartBeatTime = currentTimeMillis()
		// if haven't voted in CurrentTerm, do voteGranted and set VotedFor as the candidateId
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投赞成票，它的term是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, args.CandidateId, args.Term)
		raft.CurTermAndVotedFor.VotedFor = candidateId
		reply.VoteGranted = true
		reply.Term = recvTerm
	} else {
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投反对票，它的term是:%d, " +
			"它的lastLogIndex是:%d, 它的lastLogTerm是:%d, 而我的lastLogIndex是:%d, 我的lastLogTerm是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, args.CandidateId, args.Term,
			args.LastLogIndex, args.LastLogTerm, raft.LastLogIndex, raft.LastLogTerm)
	}
	go raft.persistState()
}

/*
	grant if the candidate has newer log entry, in the raft paper:
 	If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer (has bigger log index) is
	more up-to-date
*/
func (raft *Raft) shouldGrant(args *RequestVoteArgs) bool {
	recvLastLogIndex := args.LastLogIndex
	recvLastLogTerm := args.LastLogTerm
	if raft.CurTermAndVotedFor.VotedFor == -1 {
		if recvLastLogTerm > raft.LastLogTerm {
			return true
		} else if recvLastLogTerm == raft.LastLogTerm {
			return recvLastLogIndex >= raft.LastLogIndex
		}
	}
	return false
}

func (raft *Raft) LogAppend(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	recvTerm := args.Term
	reply.Term = raft.CurTermAndVotedFor.CurrentTerm
	if raft.CurTermAndVotedFor.CurrentTerm > recvTerm {
		log.Printf("LogAppend: raft-id: %d, 拒绝这次append，recvTerm是:%d, 而我的term是:%d", raft.me, recvTerm,
			raft.CurTermAndVotedFor.CurrentTerm)
		reply.Success = false
		return
	}
	raft.lastHeartBeatTime = currentTimeMillis()
	if raft.CurTermAndVotedFor.CurrentTerm < recvTerm || raft.isCandidate() {
		// that means current raft should change to FOLLOWER,
		// because there is another raft are doing LEADER job
		raft.stepDown(recvTerm)
		// refresh Term in reply
		reply.Term = raft.CurTermAndVotedFor.CurrentTerm
		go raft.persistState()
	}
	if !raft.isLeader() && raft.leaderId != args.LeaderId {
		raft.leaderId = args.LeaderId
	}
	success, matchIndex := raft.logConsistencyCheck(args)
	if success && len(args.Entries) > 0 {
		matchIndex = raft.appendEntries(args.Entries, matchIndex)
		go raft.persistState()
	}
	reply.Success = success
	go raft.doCommit(args.CommitIndex, matchIndex)
}

func (raft *Raft) doCommit(recvCommitIndex int, matchIndex int)  {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if raft.CommitIndex < recvCommitIndex && recvCommitIndex < len(raft.Logs) {
		var endIndex int
		if recvCommitIndex > matchIndex && raft.Logs[recvCommitIndex].Term != raft.CurTermAndVotedFor.CurrentTerm {
			endIndex = matchIndex
		} else {
			endIndex = recvCommitIndex
		}
		shouldCommitIndex := raft.CommitIndex + 1
		for shouldCommitIndex <= endIndex {
			log.Printf("LogAppend: term: %d, raft-id: %d, 将index:%d 提交到状态机",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me, shouldCommitIndex)
			raft.applyCh <- raft.Logs[shouldCommitIndex]
			shouldCommitIndex++
		}
		raft.CommitIndex = endIndex
		log.Printf("LogAppend: term: %d, raft-id: %d, 最终commitIndex是:%d, 最终matchIndex是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, raft.CommitIndex, matchIndex)
		go raft.persistState()
	}
}

func (raft *Raft) appendEntries(entries []AppendEntry, matchIndex int) int {
	log.Printf("LogAppend: term: %d, raft-id: %d, 开始append，当前matchIndex是%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.me, matchIndex)
	term := raft.CurTermAndVotedFor.CurrentTerm
	if matchIndex != raft.LastLogIndex {
		raft.Logs = raft.Logs[0:matchIndex + 1]
	}
	for _, entry := range entries {
		matchIndex++
		item := ApplyMsg{CommandValid:true, CommandIndex:matchIndex, Term:entry.Term, Command:entry.Command}
		raft.Logs = append(raft.Logs, item)
	}
	raft.LastLogIndex = len(raft.Logs) - 1
	raft.LastLogTerm = term
	log.Printf("LogAppend: term: %d, raft-id: %d, 结束append，最后matchIndex是%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.me, matchIndex)
	return matchIndex
}

func (raft *Raft) logConsistencyCheck(args *AppendEntriesArgs) (bool,int) {
	logs := raft.Logs
	index := len(logs) - 1
	if index <= 0 && args.PrevLogIndex == 0 {
		// empty log
		return true, 0
	} else {
		if args.PrevLogIndex < len(logs) {
			applyMsg := logs[args.PrevLogIndex]
			if applyMsg.Term == args.PrevLogTerm && applyMsg.CommandIndex == args.PrevLogIndex {
				return true, args.PrevLogIndex
			}
		}
	}
	return false, 0
}