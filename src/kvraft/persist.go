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
		kv.rf.PersistStateAndSnapshot(snapshotBytes)
	}
}

func (kv *KVServer) readPersistedStore() {
	snapshotBytes := kv.persister.ReadSnapshot()
	if snapshotBytes == nil || len(snapshotBytes) < 1 { // bootstrap without any snapshot
		return
	}
	buffer := bytes.NewBuffer(snapshotBytes)
	decoder := labgob.NewDecoder(buffer)
	var KvMap       		map[string]string
	var ReqSeqMap       	map[int64]int64
	var LastAppliedIndex    int
	var LastAppliedTerm     int
	err1 := decoder.Decode(&KvMap)
	err2 := decoder.Decode(&ReqSeqMap)
	err3 := decoder.Decode(&LastAppliedIndex)
	err4 := decoder.Decode(&LastAppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		log.Printf("反序列化失败！%v,%v,%v,%v", err1, err2, err3, err4)
	} else {
		kv.KvMap = KvMap
		kv.ClientReqSeqMap = ReqSeqMap
		kv.LastAppliedIndex = LastAppliedIndex
		kv.LastAppliedTerm = LastAppliedTerm
	}
}