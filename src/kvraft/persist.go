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
	err2 := encoder.Encode(kv.AppliedIndex)
	err3 := encoder.Encode(kv.AppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil {
		log.Printf("序列化失败！%v,%v,%v", err1, err2, err3)
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
	var AppliedIndex        int
	var AppliedTerm 		int
	err1 := decoder.Decode(&KvMap)
	err2 := decoder.Decode(&AppliedIndex)
	err3 := decoder.Decode(&AppliedTerm)
	if err1 != nil || err2 != nil || err3 != nil{
		log.Printf("反序列化失败！%v,%v,%v", err1, err2, err3)
	} else {
		kv.KvMap = KvMap
		kv.AppliedIndex = AppliedIndex
		kv.AppliedTerm = AppliedTerm
	}
}