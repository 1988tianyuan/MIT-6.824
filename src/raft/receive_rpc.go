package raft

import "log"

func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	recvTerm := args.Term
	recvLastLogIndex := args.LastLogIndex
	candidateId := args.CandidateId
	log.Printf("RequestVote==> term: %d, raft-id: %d, 当前votedFor是%v, 给raft-id:%d投票，它的term是:%d",
		raft.me, raft.curTermAndVotedFor.currentTerm, raft.curTermAndVotedFor.votedFor, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = raft.curTermAndVotedFor.currentTerm

	// received term is smaller, reject this request and send back currentTerm
	if recvTerm < raft.curTermAndVotedFor.currentTerm {
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d投反对票, 它的term是:%d",
			raft.me, raft.curTermAndVotedFor.currentTerm, args.CandidateId, args.Term)
		return
	}

	// received is bigger, step to FOLLOWER
	if recvTerm > raft.curTermAndVotedFor.currentTerm {
		raft.stepDown(recvTerm)
	}

	//todo: 2B  比较LastLogTerm和lastLogIndex
	if raft.curTermAndVotedFor.votedFor == -1 && recvLastLogIndex >= raft.lastLogIndex {
		// if haven't voted in currentTerm, do voteGranted and set votedFor as the candidateId
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投赞成票，它的term是:%d",
			raft.me, raft.curTermAndVotedFor.currentTerm, args.CandidateId, args.Term)
		raft.curTermAndVotedFor.votedFor = candidateId
		reply.VoteGranted = true
		reply.Term = recvTerm
	}
}

func (raft *Raft) LogAppend(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	recvTerm := args.Term
	if raft.curTermAndVotedFor.currentTerm > recvTerm {
		log.Printf("LogAppend: raft的id是:%d, 拒绝这次append，recvTerm是:%d, 而我的term是:%d", raft.me, recvTerm,
			raft.curTermAndVotedFor.currentTerm)
		reply.Term = raft.curTermAndVotedFor.currentTerm
		reply.Success = false
	} else {
		reply.Success = true
		raft.lastHeartBeatTime = currentTimeMillis()
		raft.mu.Lock()
		defer raft.mu.Unlock()
		if raft.isFollower() {
			raft.leaderId = args.LeaderId
		}
		if raft.curTermAndVotedFor.currentTerm < recvTerm {
			raft.stepDown(recvTerm)
		}
		if len(args.Entries) > 0 {
			// 2B, todo
		}
	}
}