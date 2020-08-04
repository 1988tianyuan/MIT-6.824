package raftkv

import (
	"bytes"
	"labgob"
	"log"
)

func (kv *KVServer) persistStore() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	err1 := encoder.Encode(kv.KvMap)
	err2 := encoder.Encode(kv.ClientReqSeqMap)
	err3 := encoder.Encode(kv.LastAppliedIndex)
	err4 := encoder.Encode(kv.LastAppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		log.Printf("序列化失败！%v,%v,%v,%v", err1, err2, err3, err4)
	} else {
		snapshotBytes := buffer.Bytes()
		kv.rf.WriteRaftStateAndSnapshotPersist(snapshotBytes)
	}
}

func (kv *KVServer) readPersistedStore() {
	snapshotBytes := kv.persister.ReadSnapshot()
	if snapshotBytes == nil || len(snapshotBytes) < 1 { // bootstrap without any snapshot
		return
	}
	buffer := bytes.NewBuffer(snapshotBytes)
	decoder := labgob.NewDecoder(buffer)
	kv.readPersistedKvMap(decoder)
	kv.readReqSeqMap(decoder)
	kv.readLastAppliedIndexAndTerm(decoder)
}

func (kv *KVServer) readLastAppliedIndexAndTerm(decoder *labgob.LabDecoder)  {
	var LastAppliedIndex    int
	var LastAppliedTerm     int
	err1 := decoder.Decode(&LastAppliedIndex)
	err2 := decoder.Decode(&LastAppliedTerm)
	if err1 != nil || err2 != nil {
		log.Printf("反序列化失败！%v,%v", err1, err2)
	} else {
		kv.LastAppliedIndex = LastAppliedIndex
		kv.LastAppliedTerm = LastAppliedTerm
	}
}

func (kv *KVServer) readPersistedKvMap(decoder *labgob.LabDecoder)  {
	var KvMap map[string]string
	err := decoder.Decode(&KvMap)
	if err != nil {
		log.Printf("反序列化失败！%v", err)
	} else {
		kv.KvMap = KvMap
	}
}

func (kv *KVServer) readReqSeqMap(decoder *labgob.LabDecoder)  {
	var ReqSeqMap       	map[int64]int64
	err := decoder.Decode(&ReqSeqMap)
	if err != nil {
		log.Printf("反序列化失败！%v", err)
	} else {
		kv.ClientReqSeqMap = ReqSeqMap
	}
}