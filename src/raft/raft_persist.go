package raft

import (
	"bytes"
	"labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (raft *Raft) writeRaftStatePersist() {
	data := raft.serializeRaftState()
	raft.persister.SaveRaftState(data)
	go func() {raft.LogCompactCh <- struct{}{}}()
}

func (raft *Raft) WriteRaftStateAndSnapshotPersist(snapshotBytes []byte) {
	data := raft.serializeRaftState()
	raft.persister.SaveStateAndSnapshot(data, snapshotBytes)
	go func() {raft.LogCompactCh <- struct{}{}}()
}

func (raft *Raft) serializeRaftState() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	err1 := encoder.Encode(raft.CurTermAndVotedFor)
	err2 := encoder.Encode(raft.CommitIndex)
	err3 := encoder.Encode(raft.LastLogIndex)
	err4 := encoder.Encode(raft.LastLogTerm)
	logs := raft.Logs
	err5 := encoder.Encode(logs)
	err6 := encoder.Encode(raft.LastIncludedIndex)
	err7 := encoder.Encode(raft.LastIncludedTerm)
	err8 := encoder.Encode(raft.LastAppliedIndex)
	err9 := encoder.Encode(raft.LastAppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil ||
		err4 != nil || err5 != nil || err6 != nil || err7 != nil || err8 != nil || err9 != nil {
		PrintLog("序列化失败！%v,%v,%v,%v,%v,%v,%v,%v,%v", err1, err2, err3, err4, err5, err6, err7, err8, err9)
		return nil
	}
	//PrintLog("SerializeRaftState: raft-id: %d, 准备持久化raft，当前log长度是%d, 状态是:%v",
	//	raft.Me, len(logs), raft)
	return buffer.Bytes()
}

//
// restore previously persisted state.
//
func (raft *Raft) readRaftStatePersist() {
	data := raft.persister.ReadRaftState()
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
	var LastIncludedIndex  int
	var LastIncludedTerm   int
	var LastAppliedIndex    int
	var LastAppliedTerm     int
	err1 := decoder.Decode(&CurTermAndVotedFor)
	err2 := decoder.Decode(&CommitIndex)
	err3 := decoder.Decode(&LastLogIndex)
	err4 := decoder.Decode(&LastLogTerm)
	err5 := decoder.Decode(&Logs)
	err6 := decoder.Decode(&LastIncludedIndex)
	err7 := decoder.Decode(&LastIncludedTerm)
	err8 := decoder.Decode(&LastAppliedIndex)
	err9 := decoder.Decode(&LastAppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil ||
		err4 != nil || err5 != nil || err6 != nil || err7 != nil || err8 != nil || err9 != nil {
		PrintLog("反序列化失败！%v,%v,%v,%v,%v,%v,%v,%v,%v", err1, err2, err3, err4, err5, err6, err7, err8, err9)
	} else {
		raft.LastLogIndex = LastLogIndex
		raft.LastLogTerm = LastLogTerm
		raft.CurTermAndVotedFor = CurTermAndVotedFor
		raft.CommitIndex = CommitIndex
		raft.Logs = Logs
		raft.LastIncludedIndex = LastIncludedIndex
		raft.LastIncludedTerm = LastIncludedTerm
		raft.LastAppliedIndex = LastAppliedIndex
		raft.LastAppliedTerm = LastAppliedTerm
	}
}