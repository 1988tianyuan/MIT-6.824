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
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

type State string

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type CurTermAndVotedFor struct {
	currentTerm          int		// latest term server has seen (initialized to 0on first boot, increases monotonically)
	votedFor  			 int		// voted peer id during this term
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).//todo
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                State
	isStart              bool
	lastHeartBeatTime    int64
	curTermAndVotedFor	CurTermAndVotedFor
	commitIndex  int
	lastLogIndex int
	leaderId int
}


// return currentTerm and whether this server
// believes it is the leader.
func (raft *Raft) GetState() (int, bool) {
	return raft.curTermAndVotedFor.currentTerm, raft.isLeader()
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

type RequestVoteArgs struct {
	// Your data here (2A, 2B).//todo
	Term int	// candidate’s term
	CandidateId int		// candidate requesting vote
	LastLogIndex int	// index of candidate’s last log entr
	LastLogTerm int		// term of candidate’s last log entr
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

type RequestVoteReply struct {
	// Your data here (2A).//todo
	Term int	// currentTerm, for candidate to update itself
	VoteGranted bool	// true means candidate received vote
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
		if raft.isFollower() {
			raft.leaderId = args.LeaderId
		}
		if raft.curTermAndVotedFor.currentTerm < recvTerm {
			raft.changeToFollower(recvTerm)
		}
		if len(args.Entries) > 0 {
			// 2B, todo
		}
	}
}

//
// example RequestVote RPC handler.
//
func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	changedToFollower := false
	recvTerm := args.Term
	recvLastLogIndex := args.LastLogIndex
	candidateId := args.CandidateId
	log.Printf("RequestVote: 当前raft的id是:%d, 当前term是:%d, 当前votedFor是%v, 给raft-id:%d投票，它的term是:%d",
		raft.me, raft.curTermAndVotedFor.currentTerm, raft.curTermAndVotedFor.votedFor, args.CandidateId, args.Term)

	reply.VoteGranted = false
	reply.Term = raft.curTermAndVotedFor.currentTerm

	if recvTerm < raft.curTermAndVotedFor.currentTerm {
		log.Printf("RequestVote: 当前raft的id是:%d, 当前term是:%d, raft-id:%d的term是:%d，投反对票！",
			raft.me, raft.curTermAndVotedFor.currentTerm, args.CandidateId, args.Term)
		return
	}

	if recvTerm > raft.curTermAndVotedFor.currentTerm {
		raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm:recvTerm, votedFor:-1}
		if !raft.isFollower() {
			raft.state = FOLLOWER
			changedToFollower = true
		}
	}

	//todo: 2B  比较LastLogTerm和lastLogIndex
	if raft.curTermAndVotedFor.votedFor == -1 && recvLastLogIndex >= raft.lastLogIndex {
		log.Printf("RequestVote: 当前raft的id是:%d, 当前term是:%d，给raft-id:%d 投赞成票，它的term是:%d",
					raft.me, raft.curTermAndVotedFor.currentTerm, args.CandidateId, args.Term)
		raft.curTermAndVotedFor.votedFor = candidateId
		reply.VoteGranted = true
		reply.Term = recvTerm
	}

	if changedToFollower {
		go raft.doFollowerJob()
	}
}

// need to be called in lock
func (raft *Raft) changeToFollower(term int)  {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if raft.curTermAndVotedFor.currentTerm < term {
		raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm:term, votedFor:0}
	}
	if !raft.isFollower() {
		raft.state = FOLLOWER
		go raft.doFollowerJob()
	}
}

func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return raft.peers[server].Call("Raft.RequestVote", args, reply)
}

func (raft *Raft) sendHeartbeat(leaderId int, server int, reply *AppendEntriesReply) bool {
	args := AppendEntriesArgs{Term:raft.curTermAndVotedFor.currentTerm, LeaderId:leaderId}	//2B, todo
	return raft.peers[server].Call("Raft.LogAppend", &args, reply)
}


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

	// Your code here (2B).


	return index, term, raft.isLeader()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (raft *Raft) Kill() {
	raft.isStart = false
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.me = me
	raft.state = FOLLOWER		// init with FOLLOWER state
	raft.isStart = true
	raft.readPersist(persister.ReadRaftState())

	go raft.doFollowerJob()

	//Your initialization code here (2A, 2B, 2C).//todo
	return raft
}

func (raft *Raft) doCandidateJob() {
	raft.mu.Lock()
	raft.state = CANDIDATE
	currentTerm := raft.curTermAndVotedFor.currentTerm
	raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm:currentTerm + 1, votedFor:raft.me}
	go raft.beginLeaderElection()
	raft.mu.Unlock()
	for raft.isStart && raft.isCandidate() {
		timeout := makeRandomTimeout(150, CANDIDATE_TIMEOUT_RANGE)
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		if raft.isCandidate() {
			raft.mu.Lock()
			log.Printf("doCandidateJob: 当前raft的id是：%d,当前term是: %d，选举超时, 超时时间：%d！", raft.me,
				raft.curTermAndVotedFor.currentTerm, timeout)
			currentTerm1 := raft.curTermAndVotedFor.currentTerm
			raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm:currentTerm1 + 1, votedFor:raft.me}
			go raft.beginLeaderElection()
			raft.mu.Unlock()
		}
	}
}

func (raft *Raft) beginLeaderElection() {
	if raft.isCandidate() {
		args := &RequestVoteArgs{Term:raft.curTermAndVotedFor.currentTerm, CandidateId:raft.me}
		var reply *RequestVoteReply
		votes := 1
		for server := range raft.peers {
			if server == raft.me {
				continue
			}
			reply = &RequestVoteReply{}
			ok := raft.sendRequestVote(server, args, reply)
			if ok {
				replyTerm := reply.Term
				if replyTerm > raft.curTermAndVotedFor.currentTerm {
					raft.changeToFollower(replyTerm)
					return
				}
				if reply.VoteGranted {
					log.Printf("beginLeaderElection: 当前term是%d, 当前的raft的id是%d，从raft:%d 处获得1票",
						raft.curTermAndVotedFor.currentTerm, raft.me, server)
					votes++
				}
			}
		}
		if votes >= (len(raft.peers)/2 + 1) && raft.isCandidate() {
			raft.mu.Lock()
			raft.state = LEADER
			log.Printf("选举为Leader: 当前term是%d, 当前的raft的id是%d，得到%d票",
				raft.curTermAndVotedFor.currentTerm, raft.me, votes)
			raft.mu.Unlock()
			go raft.beginLeaderJob()
		}
	}
}

func (raft *Raft) beginLeaderJob() {
	raft.doLeaderHeartBeatJob()
}

func (raft *Raft) doLeaderHeartBeatJob()  {
	for raft.isStart {
		if raft.isLeader() {
			for index := range raft.peers {
				if index == raft.me {
					continue
				}
				reply := &AppendEntriesReply{}
				if raft.sendHeartbeat(raft.me, index, reply) {
					replyTerm := reply.Term
					if !reply.Success && replyTerm > raft.curTermAndVotedFor.currentTerm {
						raft.mu.Lock()
						raft.state = FOLLOWER
						raft.curTermAndVotedFor = CurTermAndVotedFor{currentTerm:replyTerm, votedFor:-1}
						raft.mu.Unlock()
						go raft.doFollowerJob()
						return
					}
				}
			}
		} else {
			return
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

/*
	just for follower, if heartbeat from leader is timeout, begin leader election
*/
func (raft *Raft) doFollowerJob() {
	raft.lastHeartBeatTime = currentTimeMillis()
	for raft.isStart {
		timeout := makeRandomTimeout(150, 150)
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		if raft.isFollower() {
			current := currentTimeMillis()
			// leader heartbeat expired, change state to CANDIDATE and begin leader election
			if current > (raft.lastHeartBeatTime + timeout) {
				raft.printLog("FollowerWaitingJob", "FOLLOWER等待超时，转换为CANDIDATE")
				go raft.doCandidateJob()
				break
			}
		}
	}
}
