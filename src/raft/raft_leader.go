package raft

import (
	"time"
)

func (raft *Raft) changeToLeader(votes int)  {
	raft.state = LEADER
	PrintLog("BeginLeaderElection==> term: %d, raft-id: %d, 选举为LEADER, 得到%d票",
		raft.CurTermAndVotedFor.CurrentTerm, raft.Me, votes)
	go raft.doLeaderJob()
}

/*
	begin LEADER's job
*/
func (raft *Raft) doLeaderJob()  {
	if raft.OnRaftLeaderSelected != nil {
		raft.OnRaftLeaderSelected(raft)
	}
	raft.leaderInitialJob()
	raft.doHeartbeatJob()
}

/*
	sync leader's Logs to followers
*/
func (raft *Raft) syncLogsToFollowers() {
	if !raft.IsLeader() {
		return
	}
	for follower := range raft.peers {
		if follower == raft.Me || raft.raftJobMap[follower] == true {
			continue
		}
		go raft.sendAppendRequest(follower)
	}
}

func (raft *Raft) sendSnapshotRequest(follower int) {
	raft.mu.Lock()
	if !raft.IsLeader() {
		raft.mu.Unlock()
		return
	}
	snapshotData := raft.persister.ReadSnapshot()
	args := InstallSnapshotArgs{raft.LastIncludedIndex,
		raft.LastIncludedTerm, raft.CurTermAndVotedFor.CurrentTerm,
		raft.Me, snapshotData}
	reply := InstallSnapshotReply{}
	raft.mu.Unlock()
	ok := raft.peers[follower].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		raft.handleInstallSnapshotResult(reply, follower, raft.LastIncludedIndex)
	}
}

func (raft *Raft) handleInstallSnapshotResult(reply InstallSnapshotReply, follower int, sentLastIncludedIndex int) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if !raft.IsLeader() {
		return
	}
	recvTerm := reply.Term
	if recvTerm > raft.CurTermAndVotedFor.CurrentTerm {
		PrintLog("SendAppendRequest==> term: %d, raft-id: %d, 收到server: %d 的最新的term: %d, 降职为FOLLOWER",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, follower, recvTerm)
		raft.stepDown(recvTerm)
		return
	}
	if reply.Success {
		PrintLog("SendSnapshotRequest==> term: %d, raft-id: %d, 将snapshot同步到server: %d, sentLastIncludedIndex是:%d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, follower, sentLastIncludedIndex)
		if raft.matchIndex[follower] < sentLastIncludedIndex {
			raft.matchIndex[follower] = sentLastIncludedIndex
		}
		if raft.nextIndex[follower] < sentLastIncludedIndex + 1 {
			raft.nextIndex[follower] = sentLastIncludedIndex + 1
		}
	}
	raft.raftJobMap[follower] = false
}

/*
	send append request RPC to followers, from nextIndex to LastLogIndex of this LEADER
*/
func (raft *Raft) sendAppendRequest(follower int)  {
	raft.mu.Lock()
	if !raft.IsLeader() {
		raft.mu.Unlock()
		return
	}
	raft.raftJobMap[follower] = true
	// step1: init index
	latestIndex := raft.LastLogIndex
	nextIndex := raft.nextIndex[follower]
	matchIndex := raft.matchIndex[follower]
	if nextIndex <= raft.LastIncludedIndex {
		go raft.sendSnapshotRequest(follower)
		raft.mu.Unlock()
		return
	}

	PrintLog("SendAppendRequest==> term: %d, raft-id: %d, 开始向server: %d 发送AppendRequest, " +
		"matchIndex是: %d, nextIndex是 :%d",
		raft.CurTermAndVotedFor.CurrentTerm, raft.Me, follower, matchIndex, nextIndex)
	// step2: construct entries, range is from nextIndex to latestIndex
	var entries []AppendEntry
	if latestIndex >= nextIndex {
		entries = make([]AppendEntry, latestIndex - nextIndex + 1)
		entryIndex := 0
		for i := nextIndex; i <= latestIndex; i++ {
			_, applyMsg := raft.getLogEntry(i)
			entries[entryIndex] = AppendEntry{applyMsg.Command, applyMsg.CommandIndex,
				applyMsg.CommandValid, applyMsg.Term}
			entryIndex++
		}
	}
	// step3: init prevLogIndex as nextIndex - 1
	prevLogIndex, prevLogTerm := raft.makePreParams(nextIndex)
	// step4: construct AppendEntriesArgs
	request := AppendEntriesArgs{
		raft.CurTermAndVotedFor.CurrentTerm,
		raft.Me,
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
		raft.handleAppendEntryResult(reply, follower)
	}
}

func (raft *Raft) handleAppendEntryResult(reply AppendEntriesReply, follower int) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if !raft.IsLeader() {
		return
	}
	recvTerm := reply.Term
	endIndex := reply.EndIndex
	if raft.LastIncludedIndex >= reply.EndIndex {
		return
	}
	if recvTerm > raft.CurTermAndVotedFor.CurrentTerm {
		PrintLog("SendAppendRequest==> term: %d, raft-id: %d, 收到server: %d 的最新的term: %d, 降职为FOLLOWER",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, follower, recvTerm)
		raft.stepDown(recvTerm)
		return
	}
	success := reply.Success
	if success {
		PrintLog("SendAppendRequest==> term: %d, raft-id: %d, 成功将日志同步到server: %d, 最终matchIndex是: %d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, follower, endIndex)
		if raft.matchIndex[follower] < endIndex {
			raft.matchIndex[follower] = endIndex
		}
		raft.nextIndex[follower] = endIndex + 1
		go raft.checkCommit(endIndex)
	} else {
		PrintLog("SendAppendRequest==> term: %d, raft-id: %d, 无法将日志同步到server: %d, 需要更新这个nextIndex: %d",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, follower, raft.nextIndex[follower])
		raft.updateFollowerIndex(follower)	// refresh nextIndex of this follower
	}
	raft.raftJobMap[follower] = false
}

func (raft *Raft) checkCommit(endIndex int) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if raft.LastIncludedIndex >= endIndex {
		return
	}
	threshold := len(raft.peers) / 2 + 1
	oldCommitIndex := raft.CommitIndex
	reachedServers := 1
	for index := range raft.peers {
		if index == raft.Me {
			continue
		}
		matchIndex := raft.matchIndex[index]
		if matchIndex >= endIndex {
			reachedServers++
		}
	}
	// 判断需要将当前index进行commit的前提，来自raft论文：
	// a leader cannot immediately conclude that an entry from a previous term is
	// committed once it is stored on a majority of servers.
	// 如果同步的log是来自之前的term，则不能立即commit
	_, entry := raft.getLogEntry(endIndex)
	if reachedServers >= threshold && endIndex > oldCommitIndex &&
		entry.Term == raft.CurTermAndVotedFor.CurrentTerm {
		PrintLog("CheckCommit==> term: %d, raft-id: %d, index:%d 已经同步到 %d 个server, 最终commitIndex是: %d, 并提交状态机",
			raft.CurTermAndVotedFor.CurrentTerm, raft.Me, endIndex, reachedServers, endIndex)
		raft.CommitIndex = endIndex		// refresh latest commitIndex
	}
	raft.checkApply()
	raft.writeRaftStatePersist()
}

func (raft *Raft) updateFollowerIndex(follower int) {
	nextIndex := raft.nextIndex[follower]
	if nextIndex - 2 <= raft.LastIncludedIndex {
		raft.nextIndex[follower] = raft.LastIncludedIndex
		return
	}
	_, lastTimePreEntry := raft.getLogEntry(nextIndex - 1)
	updatedNextIndex := raft.LastIncludedIndex + 1
	for i := nextIndex - 2; i > raft.LastIncludedIndex; i-- {
		_, entry := raft.getLogEntry(i)
		if entry.Term != lastTimePreEntry.Term {
			updatedNextIndex = i
			break
		}
	}
	raft.nextIndex[follower] = updatedNextIndex + 1
}

/*
	for LEADER sending heartbeat to each FOLLOWER
*/
func (raft *Raft) doHeartbeatJob()  {
	t := time.NewTimer(HEARTBEAT_PERIOD)
	for raft.IsStart() && raft.IsLeader() {
		select {
		case <- t.C:
			if !raft.IsLeader() {
				return
			}
			raft.syncLogsToFollowers()
			t.Reset(HEARTBEAT_PERIOD)
			break
		//case state := <- raft.stateChangeCh:
		//	if state != LEADER {
		//		return
		//	}
		case <- raft.doRaftJobCh:
			if !raft.IsLeader() {
				return
			}
			raft.syncLogsToFollowers()
			t.Reset(HEARTBEAT_PERIOD)
			break
		}
	}
}

/*
	init each server's nextIndex as Logs's length
	init matchIndex as 0
*/
func (raft *Raft) leaderInitialJob()  {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.LeaderId = raft.Me
	raft.matchIndex = make([]int, len(raft.peers))
	raft.nextIndex = make([]int, len(raft.peers))
	if raft.UseDummyLog {
		raft.internalStart("", false)
	}
	for server := range raft.peers {
		if server == raft.Me {
			continue
		}
		raft.matchIndex[server] = 0
		raft.nextIndex[server] = raft.LastLogIndex + 1
	}
	raft.raftJobMap = make(map[int]bool)
}

func (raft *Raft) makePreParams(nextIndex int) (int,int) {
	if nextIndex - 1 == raft.LastIncludedIndex {
		return raft.LastIncludedIndex, raft.LastIncludedTerm
	} else {
		_, logEntry := raft.getLogEntry(nextIndex - 1)
		return nextIndex - 1, logEntry.Term
	}
}


