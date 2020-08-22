package raft

func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raft.mu.Lock()
	PrintLog("RequestVote:raft:%d获取了锁", raft.Me)
	defer raft.mu.Unlock()
	recvTerm := args.Term
	candidateId := args.CandidateId
	PrintLog("RequestVote==> term: %d, raft-id: %d, 当前votedFor是%v, 给raft-id:%d投票，它的term是:%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.Me, raft.CurTermAndVotedFor.VotedFor, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = raft.CurTermAndVotedFor.CurrentTerm

	// received term is smaller, reject this request and send back CurrentTerm
	if recvTerm < raft.CurTermAndVotedFor.CurrentTerm {
		PrintLog("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d投反对票, 它的term是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, args.CandidateId, args.Term)
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
		PrintLog("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投赞成票，它的term是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, args.CandidateId, args.Term)
		raft.CurTermAndVotedFor.VotedFor = candidateId
		reply.VoteGranted = true
		reply.Term = recvTerm
	} else {
		PrintLog("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投反对票，它的term是:%d, " +
			"它的lastLogIndex是:%d, 它的lastLogTerm是:%d, 而我的lastLogIndex是:%d, 我的lastLogTerm是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, args.CandidateId, args.Term,
			args.LastLogIndex, args.LastLogTerm, raft.LastLogIndex, raft.LastLogTerm)
	}
	raft.writeRaftStatePersist()
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
	PrintLog("LogAppend:raft:%d获取了锁", raft.Me)
	defer raft.mu.Unlock()
	recvTerm := args.Term
	reply.Term = raft.CurTermAndVotedFor.CurrentTerm
	if raft.CurTermAndVotedFor.CurrentTerm > recvTerm {
		PrintLog("LogAppend: raft-id: %d, 拒绝这次append，recvTerm是:%d, 而我的term是:%d", raft.Me, recvTerm,
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
		raft.writeRaftStatePersist()
	}
	if !raft.IsLeader() && raft.LeaderId != args.LeaderId {
		raft.LeaderId = args.LeaderId
	}
	success, matchIndex := raft.logConsistencyCheck(args)
	if success && len(args.Entries) > 0 {
		matchIndex = raft.appendEntries(args.Entries, matchIndex)
		raft.writeRaftStatePersist()
	}
	reply.Success = success
	go raft.doCommit(args.CommitIndex, matchIndex)
}

func (raft *Raft) doCommit(recvCommitIndex int, matchIndex int)  {
	raft.mu.Lock()
	PrintLog("doCommit:raft:%d获取了锁", raft.Me)
	defer raft.mu.Unlock()
	if raft.CommitIndex < recvCommitIndex && recvCommitIndex <= raft.LastLogIndex {
		var endIndex int
		_, entry := raft.getLogEntry(recvCommitIndex)
		if recvCommitIndex > matchIndex && entry.Term != raft.CurTermAndVotedFor.CurrentTerm {
			endIndex = matchIndex
		} else {
			endIndex = recvCommitIndex
		}
		if endIndex == 0 {
			return
		}
		raft.CommitIndex = endIndex
		PrintLog("LogAppend: term: %d, raft-id: %d, 最终commitIndex是:%d, 最终matchIndex是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, raft.CommitIndex, matchIndex)
		raft.checkApply()
		raft.writeRaftStatePersist()
	}
}

func (raft *Raft) appendEntries(entries []AppendEntry, matchIndex int) int {
	PrintLog("LogAppend: term: %d, raft-id: %d, 开始append，当前matchIndex是%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.Me, matchIndex)
	term := raft.CurTermAndVotedFor.CurrentTerm
	if matchIndex != raft.LastLogIndex {
		raft.Logs = raft.Logs[0:matchIndex - raft.LastIncludedIndex]
	}
	for _, entry := range entries {
		matchIndex++
		item := ApplyMsg{CommandValid:entry.CommandValid, CommandIndex:matchIndex, Term:entry.Term, Command:entry.Command,
			Type: APPEND_ENTRY}
		raft.Logs = append(raft.Logs, item)
	}
	raft.LastLogIndex = matchIndex
	raft.LastLogTerm = term
	PrintLog("LogAppend: term: %d, raft-id: %d, 结束append，最后matchIndex是%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.Me, matchIndex)
	return matchIndex
}

func (raft *Raft) logConsistencyCheck(args *AppendEntriesArgs) (bool,int) {
	if args.PrevLogIndex < raft.LastIncludedIndex {
		// need sync snapshot
		return false, 0
	}
	if args.PrevLogIndex == raft.LastIncludedIndex && args.PrevLogTerm == raft.LastIncludedTerm {
		return true, args.PrevLogIndex
	}
	if args.PrevLogIndex <= raft.LastLogIndex {
		success, entry := raft.getLogEntry(args.PrevLogIndex)
		if success && entry.Term == args.PrevLogTerm && entry.CommandIndex == args.PrevLogIndex {
			return true, args.PrevLogIndex
		}
	}
	return false, 0
}


func (raft *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	raft.mu.Lock()
	PrintLog("InstallSnapshot:raft:%d获取了锁", raft.Me)
	reply.Success = false
	recvTerm := args.Term
	if args.Term < raft.CurTermAndVotedFor.CurrentTerm {
		PrintLog("InstallSnapshot: raft-id: %d, InstallSnapshot，recvTerm是:%d, 而我的term是:%d", raft.Me,
			recvTerm, raft.CurTermAndVotedFor.CurrentTerm)
		reply.Term = raft.CurTermAndVotedFor.CurrentTerm
		raft.mu.Unlock()
		return
	}
	raft.lastHeartBeatTime = currentTimeMillis()
	if raft.CurTermAndVotedFor.CurrentTerm < recvTerm || raft.isCandidate() {
		// that means current raft should change to FOLLOWER,
		// because there is another raft are doing LEADER job
		raft.stepDown(recvTerm)
		// refresh Term in reply
		reply.Term = raft.CurTermAndVotedFor.CurrentTerm
		raft.writeRaftStatePersist()
	}
	if !raft.IsLeader() && raft.LeaderId != args.LeaderId {
		raft.LeaderId = args.LeaderId
	}
	reply.Success = true
	recvLastIncludedIndex := args.LastIncludedIndex
	recvLastIncludedTerm := args.LastIncludedTerm
	if raft.CommitIndex < recvLastIncludedIndex {
		raft.CommitIndex = recvLastIncludedIndex
	}
	PrintLog("InstallSnapshot: raft-id: %d, recvLastIncludedIndex是: %d, recvLastIncludedTerm是: %d", raft.Me,
		recvLastIncludedIndex, recvLastIncludedTerm)
	if recvLastIncludedIndex >= raft.LastLogIndex || recvLastIncludedIndex <= raft.LastIncludedIndex {
		raft.LastLogIndex = recvLastIncludedIndex
		raft.LastLogTerm = recvLastIncludedTerm
		// all the logs have to be compacted
		raft.Logs = make([] ApplyMsg, 0)
	} else {
		beginOffset := raft.getOffset(recvLastIncludedIndex) + 1
		raft.Logs = raft.Logs[beginOffset:]
	}
	raft.LastIncludedIndex = recvLastIncludedIndex
	raft.LastIncludedTerm = recvLastIncludedTerm
	raft.LastAppliedIndex = recvLastIncludedIndex
	raft.LastAppliedTerm = recvLastIncludedTerm
	raft.applyCh <- ApplyMsg{SnapshotData: args.SnapshotData, Type: INSTALL_SNAPSHOT}
	raft.mu.Unlock()
}