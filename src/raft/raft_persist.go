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
func (raft *Raft) writeRaftStatePersist() {
	data := raft.serializeRaftState()
	if data != nil {
		persistSeq := currentTimeMillis()
		raft.SaveStateAndSnapshotStruct(PersistStruct{data, nil, persistSeq})
		if raft.MaxStateSize > 0 && raft.persister.RaftStateSize() > (raft.MaxStateSize/2)*3 &&
			raft.LastIncludedIndex != raft.LastAppliedIndex {
			raft.logCompact()
		}
	}
}

func (raft *Raft) WriteRaftStateAndSnapshotPersist(snapshotBytes []byte) {
	data := raft.serializeRaftState()
	if data != nil {
		persistSeq := currentTimeMillis()
		raft.SaveStateAndSnapshotStruct(PersistStruct{data, snapshotBytes, persistSeq})
	}
}

func (raft *Raft) SaveStateAndSnapshotStruct(persistStruct PersistStruct) {
	raft.persistMu.Lock()
	if persistStruct.persistSeq <= raft.persistSeq {
		raft.persistMu.Unlock()
		return
	}
	raft.persistSeq = persistStruct.persistSeq
	if persistStruct.snapshot != nil {
		raft.persister.SaveStateAndSnapshot(persistStruct.raftState, persistStruct.snapshot)
	} else {
		raft.persister.SaveRaftState(persistStruct.raftState)
	}
	raft.persistMu.Unlock()
}

func (raft *Raft) serializeRaftState() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	err1 := encoder.Encode(raft.CurTermAndVotedFor)
	err2 := encoder.Encode(raft.CommitIndex)
	err3 := encoder.Encode(raft.LastLogIndex)
	err4 := encoder.Encode(raft.LastLogTerm)
	err5 := encoder.Encode(raft.Logs)
	err6 := encoder.Encode(raft.LastIncludedIndex)
	err7 := encoder.Encode(raft.LastIncludedTerm)
	err8 := encoder.Encode(raft.LastAppliedIndex)
	err9 := encoder.Encode(raft.LastAppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil ||
		err4 != nil || err5 != nil || err6 != nil || err7 != nil || err8 != nil || err9 != nil {
		log.Printf("序列化失败！%v,%v,%v,%v,%v,%v,%v,%v,%v", err1, err2, err3, err4, err5, err6, err7, err8, err9)
		return nil
	}
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
		log.Printf("反序列化失败！%v,%v,%v,%v,%v,%v,%v,%v,%v", err1, err2, err3, err4, err5, err6, err7, err8, err9)
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