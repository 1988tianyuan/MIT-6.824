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
	if err1 != nil {
		log.Printf("序列化失败！%v", err1)
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
	err1 := decoder.Decode(&KvMap)
	if err1 != nil {
		log.Printf("反序列化失败！%v", err1)
	} else {
		kv.KvMap = KvMap
	}
}