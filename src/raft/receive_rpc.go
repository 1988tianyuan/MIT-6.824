package raft

import "log"

func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.lastHeartBeatTime = currentTimeMillis() // refresh the follower's election timeout
	recvTerm := args.Term
	recvLastLogIndex := args.LastLogIndex
	recvLastLogTerm := args.LastLogTerm
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

	//todo: 2B  比较LastLogTerm和lastLogIndex
	if raft.curTermAndVotedFor.votedFor == -1 && recvLastLogIndex >= raft.lastLogIndex && recvLastLogTerm >= raft.lastLogTerm {
		// if haven't voted in currentTerm, do voteGranted and set votedFor as the candidateId
		log.Printf("RequestVote==> term: %d, raft-id: %d, 给raft-id:%d 投赞成票，它的term是:%d",
			raft.curTermAndVotedFor.currentTerm, raft.me, args.CandidateId, args.Term)
		raft.curTermAndVotedFor.votedFor = candidateId
		reply.VoteGranted = true
		reply.Term = recvTerm
	}
}

func (raft *Raft) LogAppend(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	recvTerm := args.Term
	reply.Success = false
	if raft.curTermAndVotedFor.currentTerm > recvTerm {
		log.Printf("LogAppend: raft的id是:%d, 拒绝这次append，recvTerm是:%d, 而我的term是:%d", raft.me, recvTerm,
			raft.curTermAndVotedFor.currentTerm)
		reply.Term = raft.curTermAndVotedFor.currentTerm
	} else {
		raft.lastHeartBeatTime = currentTimeMillis()
		raft.mu.Lock()
		defer raft.mu.Unlock()
		if raft.isFollower() {
			raft.leaderId = args.LeaderId
		}
		if raft.curTermAndVotedFor.currentTerm < recvTerm {
			raft.stepDown(recvTerm)
		}
		reply.Term = recvTerm
		if len(args.Commands) > 0 {
			// 2B, todo
			recvPreLogIndex := args.PrevLogIndex
			recvPreLogTerm := args.PrevLogTerm
			newCommands := args.Commands
			// follower has a index that the same with preLogIndex from leader
			if len(raft.LogEntries) > recvPreLogIndex && raft.LogEntries[recvPreLogIndex] != nil {
				lastLogEntry := raft.LogEntries[recvPreLogIndex]
				// the term of the entry has the same term with leader's prevLogTerm
				// reply success and do sync commands from leader
				if lastLogEntry.Term == recvPreLogTerm {
					reply.Success = true
					// async append newCommands to LogEntries
					go raft.doSyncCommands(newCommands, recvTerm, recvPreLogIndex)
					return
				}
			}
		}
	}
}

//TODO
func (raft *Raft) doSyncCommands(commands []interface{}, term int, recvPreLogIndex int) {
	length := len(raft.LogEntries)
	for _, command := range commands {
		index := recvPreLogIndex + 1
		entry := Entry{command, term, index}
		if index < length {
			raft.LogEntries[index] = &entry
		} else {
			raft.LogEntries = append(raft.LogEntries, &entry)
		}
		index++
	}
}
