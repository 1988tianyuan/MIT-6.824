package raft

import (
	"log"
	"time"
)

func (raft *Raft) changeToLeader(votes int)  {
	raft.state = LEADER
	log.Printf("BeginLeaderElection==> term: %d, raft-id: %d, 选举为LEADER, 得到%d票",
		raft.CurTermAndVotedFor.CurrentTerm, raft.me, votes)
	go raft.doLeaderJob()
}

/*
	begin LEADER's job
*/
func (raft *Raft) doLeaderJob()  {
	raft.initMatchIndex()
	raft.doHeartbeatJob()
}

/*
	sync leader's Logs to followers
*/
func (raft *Raft) syncLogsToFollowers() {
	if !raft.isLeader() {
		return
	}
	timeout := time.Duration(makeRandomTimeout(600, SYNC_LOG_TIMEOUT_RANGE))
	success := false
	replyChan := make(chan AppendEntriesReply, len(raft.peers) - 1)
	for follower := range raft.peers {
		if follower == raft.me {
			continue
		}
		go raft.sendAppendRequest(follower, replyChan)
	}
	timer := time.After(timeout)
	threshold := len(raft.peers)/2 + 1
	succeeded := 1
	finished := false
	endIndex := 0
	for raft.isLeader() && !finished {
		select {
		case reply := <- replyChan:
			if reply.Term > raft.CurTermAndVotedFor.CurrentTerm {
				raft.stepDown(reply.Term)
				success = false
				finished = true
				break
			}
			if reply.SendOk && raft.handleAppendRequestResult(reply, replyChan) {
				succeeded++
				if endIndex == 0 || endIndex < reply.EndIndex {
					endIndex = reply.EndIndex
				}
			}
			if succeeded >= threshold {
				success = true
				finished = true
				break
			}
		case <-timer:
			log.Printf("SendAppendRequest==> term: %d, raft-id: %d, " +
				"等待sync超时，本次agreement失败",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me)
			success = false
			finished = true
			break
		}
	}
	if success {
		log.Printf("SendAppendRequest==> term: %d, raft-id: %d, " +
			"本次agreement成功，一共有%d个raft同步成功，更新commitIndex到%d, 并apply entry",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, succeeded, endIndex)
		go raft.commitAndApply(endIndex)
	}
}

func (raft *Raft) commitAndApply(endIndex int) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	commitIndex := raft.CommitIndex
	if raft.isLeader() && endIndex > commitIndex {
		shouldCommitIndex := commitIndex + 1
		for shouldCommitIndex <= endIndex {
			log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 将index:%d 提交到状态机",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me, shouldCommitIndex)
			raft.applyCh <- raft.Logs[shouldCommitIndex]
			shouldCommitIndex++
		}
		raft.CommitIndex = endIndex		// refresh latest commitIndex
		go raft.persist()
	}
}

func (raft *Raft) handleAppendRequestResult(reply AppendEntriesReply, replyChan chan AppendEntriesReply) bool {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if reply.Term != raft.CurTermAndVotedFor.CurrentTerm {
		return false
	}
	follower := reply.FollowerPeerId
	if reply.Success {
		log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 收到server: %d 发回的AppendEntriesReply，已成功sync",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, reply.FollowerPeerId)
		if raft.matchIndex[follower] < reply.EndIndex {
			raft.matchIndex[follower] = reply.EndIndex
		}
		return true
	} else {
		// that means the matchIndex of the follower should be updated
		raft.updateMatchIndex(follower)
		return false
	}
}

/*
	send append request to followers, from nextIndex to len(raft.Logs)
*/
func (raft *Raft) sendAppendRequest(follower int, replyChan chan AppendEntriesReply)  {
	if !raft.isLeader() {
		return
	}
	// step1: init index
	latestIndex := raft.LastLogIndex
	matchIndex := raft.matchIndex[follower]
	log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 开始向server: %d 发送AppendRequest, matchIndex是: %d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.me, follower, matchIndex)

	// step2: construct entries, range is from matchIndex + 1 to latestIndex
	entries := make([]AppendEntry, latestIndex - matchIndex)
	entryIndex := 0
	for i := matchIndex + 1; i <= latestIndex; i++ {
		applyMsg := raft.Logs[i]
		entries[entryIndex] = AppendEntry{applyMsg.Command, applyMsg.CommandIndex,
			applyMsg.Term}
		entryIndex++
	}

	// step3: init prevLogIndex as matchIndex
	prevLogIndex, prevLogTerm := raft.makePreParams(matchIndex)

	// step4: construct AppendEntriesArgs
	request := AppendEntriesArgs{
		raft.CurTermAndVotedFor.CurrentTerm,
		raft.me,
		prevLogIndex,
		prevLogTerm,
		entries,
		raft.CommitIndex}
	// step5: send AppendEntries rpc request
	reply := AppendEntriesReply{FollowerPeerId:follower, EndIndex:latestIndex}
	ok := raft.peers[follower].Call("Raft.LogAppend", &request, &reply)
	reply.SendOk = ok
	replyChan <- reply
}

func (raft *Raft) updateMatchIndex(follower int) {
	matchIndex := raft.matchIndex[follower]
	term := raft.Logs[matchIndex].Term
	updatedIndex := 0
	for i := matchIndex - 1; i >= 0; i-- {
		if raft.Logs[i].Term != term {
			updatedIndex = i
			break
		}
	}
	raft.matchIndex[follower] = updatedIndex
}

/*
	for LEADER sending heartbeat to each FOLLOWER
*/
func (raft *Raft) doHeartbeatJob()  {
	for raft.isStart && raft.isLeader() {
		// send heartbeat to each follower
		//for index := range raft.peers {
		//	if index == raft.me {
		//		continue
		//	}
		//	go raft.sendHeartbeat(index)
		//}
		go raft.syncLogsToFollowers()
		time.Sleep(HEARTBEAT_PERIOD)
	}
}

/*
	send heartbeat rpc request with empty entries
*/
func (raft *Raft) sendHeartbeat(follower int) {
	prevLogIndex, prevLogTerm := raft.makePreParams(raft.matchIndex[follower])
	args := AppendEntriesArgs{
		Term:raft.CurTermAndVotedFor.CurrentTerm,
		LeaderId:raft.me,
		PrevLogTerm:prevLogTerm,
		PrevLogIndex:prevLogIndex,
		CommitIndex:raft.CommitIndex}
	reply := AppendEntriesReply{}
	ok := raft.peers[follower].Call("Raft.LogAppend", &args, &reply)
	if ok {
		raft.mu.Lock()
		defer raft.mu.Unlock()
		if reply.Term > raft.CurTermAndVotedFor.CurrentTerm {
			raft.stepDown(reply.Term)
		}
	}
}

/*
	init each server's nextIndex as Logs's length
	init matchIndex as 0
*/
func (raft *Raft) initMatchIndex()  {
	raft.matchIndex = make([]int, len(raft.peers))
	for server := range raft.matchIndex {
		if server == raft.me {
			continue
		}
		total := len(raft.Logs)
		if total == 0 {
			raft.matchIndex[server] = 0
		} else {
			raft.matchIndex[server] = raft.Logs[total - 1].CommandIndex
		}
	}
}

func (raft *Raft) makePreParams(matchIndex int) (int,int) {
	if len(raft.Logs) <= 0 {
		return 0, 0
	} else {
		return matchIndex, raft.Logs[matchIndex].Term
	}
}


