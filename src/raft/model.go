package raft

import (
	"labrpc"
	"sync"
)

type State string

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state\
	applyCh   chan ApplyMsg
	me        int // this peer's index into peers[]
	state              State
	isStart            bool
	lastHeartBeatTime  int64
	leaderId           int
	matchIndex         []int
	nextIndex		   []int
	lastApplied        int
	LastLogIndex       int
	LastLogTerm        int
	CurTermAndVotedFor CurTermAndVotedFor
	CommitIndex        int
	Logs               []ApplyMsg
}

func (raft *Raft) GetState() (int, bool) {
	return raft.CurTermAndVotedFor.CurrentTerm, raft.isLeader()
}

type CurTermAndVotedFor struct {
	CurrentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int // voted peer id during this term
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).//todo
	Term int	// candidate’s term
	CandidateId int		// candidate requesting vote
	LastLogIndex int	// index of candidate’s last log entr
	LastLogTerm int		// term of candidate’s last log entr
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [] AppendEntry
	CommitIndex  int
}

type AppendEntry struct {
	Command      interface{}
	CommandIndex int
	Term 		 int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	FollowerPeerId int
	EndIndex int
	SendOk bool
}

type RequestVoteReply struct {
	Term int	// CurrentTerm, for candidate to update itself
	VoteGranted bool	// true means candidate received vote
	Server int
	HasStepDown bool
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term 		 int
}

type SnapShot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Command   interface{}
}