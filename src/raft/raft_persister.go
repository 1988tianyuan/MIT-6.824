package raft

import (
	"bytes"
	"labgob"
	"log"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (raft *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	err1 := encoder.Encode(raft.CurTermAndVotedFor)
	err2 := encoder.Encode(raft.CommitIndex)
	err3 := encoder.Encode(raft.LastLogIndex)
	err4 := encoder.Encode(raft.LastLogTerm)
	err5 := encoder.Encode(raft.Logs)
	if err1 != nil || err2 != nil || err3 != nil ||
		err4 != nil || err5 != nil {
		log.Printf("序列化失败！%v,%v,%v,%v,%v", err1, err2, err3, err4, err5)
	} else {
		data := buffer.Bytes()
		raft.persister.SaveRaftState(data)
	}
}


//
// restore previously persisted state.
//
func (raft *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var LastLogIndex       int
	var LastLogTerm        int
	var CurTermAndVotedFor CurTermAndVotedFor
	var CommitIndex        int
	var Logs               []ApplyMsg
	err1 := decoder.Decode(&CurTermAndVotedFor)
	err2 := decoder.Decode(&CommitIndex)
	err3 := decoder.Decode(&LastLogIndex)
	err4 := decoder.Decode(&LastLogTerm)
	err5 := decoder.Decode(&Logs)
	if err1 != nil || err2 != nil || err3 != nil ||
		err4 != nil || err5 != nil {
		log.Printf("反序列化失败！%v,%v,%v,%v,%v", err1, err2, err3, err4, err5)
	} else {
		raft.LastLogIndex = LastLogIndex
		raft.LastLogTerm = LastLogTerm
		raft.CurTermAndVotedFor = CurTermAndVotedFor
		raft.CommitIndex = CommitIndex
		raft.Logs = Logs
	}
}