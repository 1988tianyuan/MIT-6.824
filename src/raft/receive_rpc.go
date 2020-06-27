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
		raft.curTermAndVotedFor.currentTerm, raft.me, raft.curTermAndVotedFor.votedFor, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = raft.curTermAndVotedFor.currentTerm

	// received term is smaller, reject this request and send back currentTerm
	if recvTerm < raft.curTermAndVotedFor.currentTerm {
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d投反对票, 它的term是:%d",
			raft.curTermAndVotedFor.currentTerm, raft.me, args.CandidateId, args.Term)
		return
	}

	// received is bigger, step to FOLLOWER
	if recvTerm > raft.curTermAndVotedFor.currentTerm {
		raft.stepDown(recvTerm)
	}

	if raft.shouldGrant(args) {
		// refresh the follower's election timeout
		raft.lastHeartBeatTime = currentTimeMillis()
		// if haven't voted in currentTerm, do voteGranted and set votedFor as the candidateId
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投赞成票，它的term是:%d",
			raft.curTermAndVotedFor.currentTerm, raft.me, args.CandidateId, args.Term)
		raft.curTermAndVotedFor.votedFor = candidateId
		reply.VoteGranted = true
		reply.Term = recvTerm
	} else {
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投反对票，它的term是:%d, " +
			"它的lastLogIndex是:%d, 它的lastLogTerm是:%d, 而我的lastLogIndex是:%d, 我的lastLogTerm是:%d",
			raft.curTermAndVotedFor.currentTerm, raft.me, args.CandidateId, args.Term,
			args.LastLogIndex, args.LastLogTerm, raft.lastLogIndex, raft.lastLogTerm)
	}
}

func (raft *Raft) shouldGrant(args *RequestVoteArgs) bool {
	recvLastLogIndex := args.LastLogIndex
	recvLastLogTerm := args.LastLogTerm
	if raft.curTermAndVotedFor.votedFor == -1 {
		if recvLastLogTerm > raft.lastLogTerm {
			return true
		} else if recvLastLogTerm == raft.lastLogTerm {
			return recvLastLogIndex >= raft.lastLogIndex
		}
	}
	return false
}

func (raft *Raft) LogAppend(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	recvTerm := args.Term
	shouldAppend, matchIndex := raft.shouldAppendEntries(args)
	if raft.curTermAndVotedFor.currentTerm > recvTerm {
		log.Printf("LogAppend: raft的id是:%d, 拒绝这次append，recvTerm是:%d, 而我的term是:%d", raft.me, recvTerm,
			raft.curTermAndVotedFor.currentTerm)
		reply.Term = raft.curTermAndVotedFor.currentTerm
		reply.Success = false
	} else if shouldAppend {
		reply.Success = true
		raft.lastHeartBeatTime = currentTimeMillis()
		raft.mu.Lock()
		defer raft.mu.Unlock()
		if raft.isFollower() && raft.leaderId != args.LeaderId {
			raft.leaderId = args.LeaderId
		}
		if raft.curTermAndVotedFor.currentTerm < recvTerm {
			raft.stepDown(recvTerm)
		}
		if len(args.Entries) > 0 {
			matchIndex = raft.appendEntries(args.Entries, matchIndex)
		}
		if raft.commitIndex != args.CommitIndex && args.CommitIndex < len(raft.logs) {
			var endIndex int
			if args.CommitIndex > matchIndex {
				endIndex = matchIndex
			} else {
				endIndex = args.CommitIndex
			}
			shouldCommitIndex := raft.commitIndex + 1
			for shouldCommitIndex <= endIndex {
				log.Printf("LogAppend: term: %d, raft-id: %d, 将index:%d 提交到状态机",
					raft.curTermAndVotedFor.currentTerm, raft.me, shouldCommitIndex)
				raft.applyCh <- raft.logs[shouldCommitIndex]
				shouldCommitIndex++
			}
			raft.commitIndex = endIndex
			log.Printf("LogAppend: term: %d, raft-id: %d, 最终commitIndex是:%d",
				raft.curTermAndVotedFor.currentTerm, raft.me, raft.commitIndex)
		}
	} else {
		reply.Success = false
	}
}

func (raft *Raft) appendEntries(entries []interface{}, matchIndex int) int {
	log.Printf("LogAppend: term: %d, raft-id: %d, 开始append，当前matchIndex是%d",
		raft.curTermAndVotedFor.currentTerm, raft.me, matchIndex)
	currentIndex := matchIndex + 1
	term := raft.curTermAndVotedFor.currentTerm
	for _, entry := range entries {
		item := ApplyMsg{CommandValid:true, CommandIndex:currentIndex, Term:term, Command:entry}
		if currentIndex < len(raft.logs) {
			raft.logs[currentIndex] = item
		} else {
			raft.logs = append(raft.logs, item)
		}
		currentIndex++
	}
	raft.lastLogIndex = len(raft.logs) - 1
	raft.lastLogTerm = term
	return currentIndex - 1
}

func (raft *Raft) shouldAppendEntries(args *AppendEntriesArgs) (bool,int) {
	logs := raft.logs
	index := len(logs) - 1
	if index <= 0 {
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