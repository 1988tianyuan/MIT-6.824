package raft

import (
	"labrpc"
	"sync"
)

type State string

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
	lastApplied  int
	lastLogIndex int
	lastLogTerm int
	leaderId 	 int
	matchIndex []int
	nextIndex  []int
	logs	   []LogItem
}

func (raft *Raft) GetState() (int, bool) {
	return raft.curTermAndVotedFor.currentTerm, raft.isLeader()
}

type CurTermAndVotedFor struct {
	currentTerm          int		// latest term server has seen (initialized to 0on first boot, increases monotonically)
	votedFor  			 int		// voted peer id during this term
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
	Entries      [] interface{}
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

type RequestVoteReply struct {
	Term int	// currentTerm, for candidate to update itself
	VoteGranted bool	// true means candidate received vote
	Server int
	HasStepDown bool
}

type LogItem struct {
	Term int
	Index int
	command interface{}
}

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