package raft

import (
	"log"
	"time"
)

func (raft *Raft) changeToLeader(votes int)  {
	raft.state = LEADER
	log.Printf("BeginLeaderElection==> term: %d, raft-id: %d, 选举为LEADER, 得到%d票",
		raft.curTermAndVotedFor.currentTerm, raft.me, votes)
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
	sync leader's logs to followers
*/
func (raft *Raft) syncLogsToFollowers(timeout time.Duration) {
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
			if reply.Term > raft.curTermAndVotedFor.currentTerm {
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
				raft.curTermAndVotedFor.currentTerm, raft.me)
			success = false
			finished = true
			break
		}
	}
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if success && raft.isLeader() && endIndex > raft.commitIndex {
		log.Printf("SendAppendRequest==> term: %d, raft-id: %d, " +
			"本次agreement成功，一共有%d个raft同步成功，更新commitIndex到%d, 并apply entry",
			raft.curTermAndVotedFor.currentTerm, raft.me, succeeded, endIndex)
		shouldCommitIndex := raft.commitIndex + 1
		for shouldCommitIndex <= endIndex {
			log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 将index:%d 提交到状态机",
				raft.curTermAndVotedFor.currentTerm, raft.me, shouldCommitIndex)
			raft.applyCh <- raft.logs[shouldCommitIndex]
			shouldCommitIndex++
		}
		raft.commitIndex = endIndex
	}
}

func (raft *Raft) handleAppendRequestResult(reply AppendEntriesReply, replyChan chan AppendEntriesReply) bool {
	follower := reply.FollowerPeerId
	if reply.Success {
		log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 收到server: %d 发回的AppendEntriesReply，已成功sync",
			raft.curTermAndVotedFor.currentTerm, raft.me, reply.FollowerPeerId)
		if raft.matchIndex[follower] < reply.EndIndex {
			raft.matchIndex[follower] = reply.EndIndex
		}
		return true
	} else {
		// that means the matchIndex of the follower should be updated
		raft.updateMatchIndex(follower)
		go raft.sendAppendRequest(follower, replyChan)
		return false
	}
}

/*
	send append request to followers, from nextIndex to len(raft.logs)
*/
func (raft *Raft) sendAppendRequest(follower int, replyChan chan AppendEntriesReply)  {
	if !raft.isLeader() {
		return
	}
	// step1: init index
	latestIndex := raft.lastLogIndex
	matchIndex := raft.matchIndex[follower]
	log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 开始向server: %d 发送AppendRequest, matchIndex是: %d",
		raft.curTermAndVotedFor.currentTerm, raft.me, follower, matchIndex)

	// step2: construct entries, range is from matchIndex + 1 to latestIndex
	entries := make([]interface{}, latestIndex - matchIndex)
	entryIndex := 0
	for i := matchIndex + 1; i <= latestIndex; i++ {
		entries[entryIndex] = raft.logs[i].Command
		entryIndex++
	}

	// step3: init prevLogIndex as matchIndex
	prevLogIndex, prevLogTerm := raft.makePreParams(matchIndex)

	// step4: construct AppendEntriesArgs
	request := AppendEntriesArgs{
		raft.curTermAndVotedFor.currentTerm,
		raft.me,
		prevLogIndex,
		prevLogTerm,
		entries,
		raft.commitIndex}
	// step5: send AppendEntries rpc request
	reply := AppendEntriesReply{FollowerPeerId:follower, EndIndex:latestIndex, SendOk:true}
	ok := raft.peers[follower].Call("Raft.LogAppend", &request, &reply)
	if !ok {
		log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 向server: %d 发送AppendRequest失败了",
			raft.curTermAndVotedFor.currentTerm, raft.me, follower)
		reply.SendOk = false
	}
	replyChan <- reply
}

func (raft *Raft) updateMatchIndex(follower int) {
	matchIndex := raft.matchIndex[follower]
	term := raft.logs[matchIndex].Term
	updatedIndex := 0
	for i := matchIndex - 1; i >= 0; i-- {
		if raft.logs[i].Term != term {
			updatedIndex = i
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
		for index := range raft.peers {
			if index == raft.me {
				continue
			}
			go raft.sendHeartbeat(index)
		}
		time.Sleep(HEARTBEAT_PERIOD)
	}
}

/*
	send heartbeat rpc request with empty entries
*/
func (raft *Raft) sendHeartbeat(follower int) {
	prevLogIndex, prevLogTerm := raft.makePreParams(raft.matchIndex[follower])
	args := AppendEntriesArgs{
		Term:raft.curTermAndVotedFor.currentTerm,
		LeaderId:raft.me,
		PrevLogTerm:prevLogTerm,
		PrevLogIndex:prevLogIndex,
		CommitIndex:raft.commitIndex}
	reply := AppendEntriesReply{}
	ok := raft.peers[follower].Call("Raft.LogAppend", &args, &reply)
	raft.mu.Lock()
	if ok && reply.Term > raft.curTermAndVotedFor.currentTerm {
		raft.stepDown(reply.Term)
	}
	defer raft.mu.Unlock()
}

/*
	init each server's nextIndex as logs's length
	init matchIndex as 0
*/
func (raft *Raft) initMatchIndex()  {
	raft.matchIndex = make([]int, len(raft.peers))
	for server := range raft.matchIndex {
		if server == raft.me {
			continue
		}
		total := len(raft.logs)
		if total == 0 {
			raft.matchIndex[server] = 0
		} else {
			raft.matchIndex[server] = raft.logs[total - 1].CommandIndex
		}
	}
}

func (raft *Raft) makePreParams(matchIndex int) (int,int) {
	if len(raft.logs) <= 0 {
		return 0, 0
	} else {
		return matchIndex, raft.logs[matchIndex].Term
	}
}


