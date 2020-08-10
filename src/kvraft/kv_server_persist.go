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
	if err1 != nil || err2 != nil {
		log.Printf("序列化失败！%v,%v", err1, err2)
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