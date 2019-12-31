package raft

import (
	"log"
	"time"
)

/*
	begin LEADER's job
*/
func (raft *Raft) doLeaderJob()  {
	raft.initNextIndex()
	raft.doHeartbeatJob()
}

func (raft *Raft) changeToLeader(votes int)  {
	raft.state = LEADER
	log.Printf("BeginLeaderElection==> term: %d, raft-id: %d, 选举为LEADER, 得到%d票",
		raft.curTermAndVotedFor.currentTerm, raft.me, votes)
	go raft.doLeaderJob()
}

/*
	sync leader's logs to followers
*/
func (raft *Raft) syncLogsToFollowers() {
	for server := range raft.peers {
		if server == raft.me {
			continue
		}
		go raft.sendAppendRequest(server)
	}
}

/*
	send append request to followers, from nextIndex to len(raft.logs)
*/
func (raft *Raft) sendAppendRequest(follower int)  {
	// step1: init index
	latestIndex := len(raft.logs)
	nextIndex := raft.nextIndex[follower]
	matchIndex := raft.matchIndex[follower]
	log.Printf("SendAppendRequest==> term: %d, raft-id: %d, 开始向server: %d 发送AppendRequest, nextIndex是: %d",
		raft.curTermAndVotedFor.currentTerm, raft.me, follower, nextIndex)

	// step2: construct entries, range is from nextIndex to latestIndex
	entries := make([]interface{}, Min(latestIndex, nextIndex) - matchIndex)
	entryIndex := 0
	for i := matchIndex; i < Min(latestIndex, nextIndex); i++ {
		entries[entryIndex] = raft.logs[i].Command
		entryIndex++
	}

	// step3: init prevLogIndex as nextIndex - 1
	prevLogIndex, prevLogTerm := raft.makePreParams(matchIndex)

	// step4: construct AppendEntriesArgs
	request := AppendEntriesArgs{
		raft.curTermAndVotedFor.currentTerm,
		raft.me,
		prevLogIndex,
		prevLogTerm,
		entries,
		raft.commitIndex}
	reply := AppendEntriesReply{}
	// step5: send AppendEntries rpc request
	go raft.sendAndHandle(follower, &request, &reply, latestIndex, nextIndex, matchIndex)
}

func (raft *Raft) sendAndHandle(follower int, request *AppendEntriesArgs, reply *AppendEntriesReply,
	latestIndex int, nextIndex int, matchIndex int)  {
	ok := raft.peers[follower].Call("Raft.LogAppend", request, reply)
	if ok {
		success := reply.Success
		if reply.Term > raft.curTermAndVotedFor.currentTerm {
			raft.stepDown(reply.Term)
			return
		}
		if !success {
			// that means the matchIndex of the follower should be updated
			raft.matchIndex[follower] = raft.updateMatchIndex(matchIndex, raft.logs[matchIndex].Term)
			go raft.sendAppendRequest(follower)
		} else {
			raft.nextIndex[follower] = latestIndex
			raft.matchIndex[follower] = nextIndex - 1
		}
	}
}

/*
	update nextIndex as last term's log index
*/
func (raft *Raft) updateNextIndex(nextIndex int, term int) int {
	updatedIndex := 0
	for i := nextIndex - 1; i >= 0; i-- {
		if raft.logs[i].Term != term {
			updatedIndex = i
		}
	}
	return updatedIndex
}

func (raft *Raft) updateMatchIndex(matchIndex int, term int) int {
	updatedIndex := 0
	for i := matchIndex - 1; i >= 0; i-- {
		if raft.logs[i].Term != term {
			updatedIndex = i
		}
	}
	return updatedIndex
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
func (raft *Raft) initNextIndex()  {
	raft.nextIndex = make([]int, len(raft.peers))
	raft.matchIndex = make([]int, len(raft.peers))
	for server := range raft.nextIndex {
		if server == raft.me {
			continue
		}
		if len(raft.logs) == 0 {
			raft.nextIndex[server] = 1
		} else {
			raft.nextIndex[server] = len(raft.logs)
		}
		raft.matchIndex[server] = 0
	}
}

func (raft *Raft) makePreParams(matchIndex int) (int,int) {
	if len(raft.logs) <= 0 {
		return 0, 0
	} else {
		return  matchIndex, raft.logs[matchIndex].Term
	}
}


