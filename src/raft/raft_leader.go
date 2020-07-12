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
	for follower := range raft.peers {
		if follower == raft.me {
			continue
		}
		go raft.sendAppendRequest(follower)
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
func (raft *Raft) sendAppendRequest(follower int)  {
	raft.mu.Lock()
	if !raft.isLeader() {
		raft.mu.Unlock()
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
	raft.mu.Unlock()

	// step6: begin RPC calling
	ok := raft.peers[follower].Call("Raft.LogAppend", &request, &reply)
	if ok {
		raft.mu.Lock()
		defer raft.mu.Unlock()
		if reply.Term > raft.CurTermAndVotedFor.CurrentTerm {
			log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 收到server: %d 的最新的term: %d, 降职为FOLLOWER",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me, follower, reply.Term)
			raft.stepDown(reply.Term)
			return
		}
		success := reply.Success
		if success {
			log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 成功将日志同步到server: %d, 最终matchIndex是: %d",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me, follower, reply.EndIndex)
			if raft.matchIndex[follower] < reply.EndIndex {
				raft.matchIndex[follower] = reply.EndIndex
			}
			go raft.checkCommit(reply.EndIndex)
		} else {
			log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 无法将日志同步到server: %d, 需要更新matchIndex: %d",
				raft.CurTermAndVotedFor.CurrentTerm, raft.me, follower, reply.EndIndex)
			raft.updateMatchIndex(follower)
			go raft.sendAppendRequest(follower)
		}
	}
}

func (raft *Raft) checkCommit(endIndex int) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	threshold := len(raft.peers) / 2 + 1
	commitIndex := raft.CommitIndex
	reachedServers := 1
	for index := range raft.peers {
		if index == raft.me {
			continue
		}
		matchIndex := raft.matchIndex[index]
		if matchIndex >= endIndex {
			reachedServers++
		}
	}
	if reachedServers >= threshold && endIndex > commitIndex {
		log.Printf("CheckCommit==> term: %d, raft-id: %d, index:%d 已经同步到 %d 个server, 最终commitIndex是: %d, 并提交状态机",
			raft.CurTermAndVotedFor.CurrentTerm, raft.me, endIndex, reachedServers, endIndex)
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
		go raft.syncLogsToFollowers()
		time.Sleep(HEARTBEAT_PERIOD)
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


