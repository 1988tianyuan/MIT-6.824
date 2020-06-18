package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (raft *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	//todo: Your code here (2B).
	raft.doAgreement(command)
	return index, term, raft.isLeader()
}

func (raft *Raft) doAgreement(command interface{}) {
	index := len(raft.LogEntries)
	raft.LogEntries = append(raft.LogEntries, &Entry{command, raft.curTermAndVotedFor.currentTerm, index})
	successNum := raft.doSyncToFollowers()
	if successNum >= (len(raft.peers)/2 + 1) {
		raft.commitIndex = len(raft.LogEntries) - 1
		applyMsg := ApplyMsg{true, command, raft.commitIndex}
		raft.applyCh <- applyMsg
	}
}

func (raft *Raft) doSyncToFollowers() int {
	for peerId := range raft.peers {
		if peerId != raft.me {
			state := raft.followerSyncStates[peerId]

		}
	}
}

func (raft *Raft) doCandidateJob() {
	for raft.isStart {
		raft.mu.Lock()
		raft.state = CANDIDATE
		currentTerm := raft.curTermAndVotedFor.currentTerm
		raft.curTermAndVotedFor =
			CurTermAndVotedFor{currentTerm: currentTerm + 1, votedFor: raft.me} // increment term and vote for self
		timeout := makeRandomTimeout(150, CANDIDATE_TIMEOUT_RANGE) // random election timeout
		go raft.beginLeaderElection(timeout)
		raft.mu.Unlock()
		time.Sleep(timeout)
		if raft.isCandidate() { // election timeout
			log.Printf("DoCandidateJob==> term: %d, raft-id: %d, 选举超时, 重新开始选举",
				raft.curTermAndVotedFor.currentTerm, raft.me)
		} else {
			break
		}
	}
}

func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyChan chan RequestVoteReply) {
	reply := RequestVoteReply{}
	reply.HasStepDown = false
	reply.Server = server
	ok := raft.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		replyTerm := reply.Term
		if replyTerm > raft.curTermAndVotedFor.currentTerm {
			raft.mu.Lock()
			defer raft.mu.Unlock()
			raft.stepDown(replyTerm)
			reply.HasStepDown = true
		}
		replyChan <- reply
	}
}

func (raft *Raft) beginLeaderElection(duration time.Duration) {
	replyChan := make(chan RequestVoteReply, len(raft.peers)-1) // channel for receive async vote request
	if raft.isCandidate() {
		args := &RequestVoteArgs{Term: raft.curTermAndVotedFor.currentTerm, CandidateId: raft.me}
		votes := 1
		for server := range raft.peers {
			if server == raft.me {
				continue
			}
			go raft.sendRequestVote(server, args, replyChan)
		}
		timer := time.After(duration)
		threshold := len(raft.peers)/2 + 1
		for raft.isCandidate() {
			select {
			case reply := <-replyChan:
				if reply.HasStepDown {
					return
				}
				if reply.VoteGranted {
					log.Printf("BeginLeaderElection==> term: %d, raft-id: %d, 从raft:%d 处获得1票",
						raft.curTermAndVotedFor.currentTerm, raft.me, reply.Server)
					votes++
				}
				if votes >= threshold {
					raft.mu.Lock()
					raft.changeToLeader(votes)
					raft.mu.Unlock()
					return
				}
			case <-timer:
				return
			}
		}
	}
}

func (raft *Raft) changeToLeader(votes int) {
	raft.state = LEADER
	log.Printf("BeginLeaderElection==> term: %d, raft-id: %d, 选举为LEADER, 得到%d票",
		raft.curTermAndVotedFor.currentTerm, raft.me, votes)
	go raft.doLeaderJob()
}

/*
	begin LEADER's job
*/
func (raft *Raft) doLeaderJob() {
	raft.doHeartbeatJob()
}

// need to be called in lock, and the term should be bigger than raft's currentTerm
func (raft *Raft) stepDown(term int) {
	raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm: term, votedFor: -1}
	if !raft.isFollower() {
		raft.state = FOLLOWER
		go raft.doFollowerJob()
	}
}

/*
	for LEADER sending heartbeat to each FOLLOWER
*/
func (raft *Raft) doHeartbeatJob() {
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

func (raft *Raft) sendHeartbeat(follower int) {
	args := AppendEntriesArgs{Term: raft.curTermAndVotedFor.currentTerm, LeaderId: raft.me} //2B, todo
	reply := AppendEntriesReply{}
	ok := raft.peers[follower].Call("Raft.LogAppend", &args, &reply)
	raft.mu.Lock()
	if !ok && reply.Term > raft.curTermAndVotedFor.currentTerm {
		raft.stepDown(reply.Term)
	}
	defer raft.mu.Unlock()
}

/*
	just for follower, if heartbeat from leader is timeout, begin leader election
*/
func (raft *Raft) doFollowerJob() {
	raft.lastHeartBeatTime = currentTimeMillis()
	for raft.isStart {
		timeout := makeRandomTimeout(150, 150)
		time.Sleep(timeout)
		if raft.isFollower() {
			current := currentTimeMillis()
			// leader heartbeat expired, change state to CANDIDATE and begin leader election
			if current > (raft.lastHeartBeatTime + timeout.Nanoseconds()/1000000) {
				log.Printf("DoFollowerJob==> term: %d, raft-id: %d, FOLLOWER等待超时，转换为CANDIDATE",
					raft.curTermAndVotedFor.currentTerm, raft.me)
				go raft.doCandidateJob()
				break
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.me = me
	raft.state = FOLLOWER // init with FOLLOWER state
	raft.isStart = true
	raft.readPersist(persister.ReadRaftState())
	raft.LogEntries = make([]*Entry, 0)
	raft.applyCh = applyCh

	go raft.doFollowerJob()

	//Your initialization code here (2A, 2B, 2C).//todo
	return raft
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (raft *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (raft *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}
