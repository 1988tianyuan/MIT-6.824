package raft

import (
	"labrpc"
	"sync"
)

type State string

type Raft struct {
	mu                 sync.RWMutex          // Lock to protect shared access to this peer's state
	persistMu		   sync.RWMutex
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state\
	applyCh            chan ApplyMsg
	doRaftJobCh		   chan struct{}
	stateChangeCh	   chan State
	state              State
	lastHeartBeatTime  int64
	matchIndex         []int
	nextIndex          []int
	raftJobMap		   sync.Map
	OnRaftLeaderSelected func(*Raft)

	// public properties, need persisted
	LastLogIndex       int
	LastLogTerm        int
	CurTermAndVotedFor CurTermAndVotedFor
	CommitIndex        int
	Logs               []ApplyMsg
	LastIncludedIndex  int
	LastIncludedTerm   int
	LastAppliedIndex int
	LastAppliedTerm int

	// other public properties
	UseDummyLog		   bool
	LeaderId           int
	Me                 int // this peer's index into peers[]
	LogCompactCh	   chan struct{}
}

type CurTermAndVotedFor struct {
	CurrentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int // voted peer id during this term
}

type RequestVoteArgs struct {
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
	CommandValid bool
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
	Type         MsgType
	SnapshotData []byte
}

type MsgType string

type InstallSnapshotArgs struct {
	LastIncludedIndex  int
	LastIncludedTerm   int
	Term              int
	LeaderId          int
	SnapshotData      []byte
}

type InstallSnapshotReply struct {
	Term int
	Success bool
}

type PersistStruct struct {
	raftState  []byte
	snapshot   []byte
	persistSeq int64
}