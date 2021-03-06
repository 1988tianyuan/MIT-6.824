package raftkv

import (
	"labrpc"
	"time"
)

func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	return ck.doRPCRequest(&args, "KVServer.Get").(string)
}

func(ck *Clerk) PutAppend(key string, value string, op Operation) {
	ck.requestSeq++
	args := PutAppendArgs{key, value, op, ck.requestSeq, ck.clientId}
	ck.doRPCRequest(&args, "KVServer.PutAppend")
}

func(ck *Clerk) doRPCRequest(args interface{}, rpcTarget string) interface{} {
	success, content := ck.request(args, rpcTarget)
	if !success {
		ck.refreshLeaderIndex()
		// sleep 50ms and retry
		time.Sleep(50 * time.Millisecond)
		return ck.doRPCRequest(args, rpcTarget)
	} else {
		return content
	}
}

func(ck *Clerk) request(args interface{}, rpcTarget string) (bool, interface{}) {
	if ck.leaderIndex == -1 {
		return false, nil
	}
	reply := CommonReply{}
	ok := ck.servers[ck.leaderIndex].Call(rpcTarget, args, &reply)
	success := ok && !reply.WrongLeader && len(reply.Err) <= 0
	if !success {
		PrintLog("%v: 本次发送失败，发送的args是:%v，收到的reply是:%v, error是:%v, 是不是发到错误leader了:%v",
			rpcTarget, args, reply, reply.Err, reply.WrongLeader)
	} else {
		PrintLog("%v: 本次发送成功，发送的args是:%v，收到的reply是:%v",
			rpcTarget, args, reply)
	}
	return success, reply.Content
}

func(ck *Clerk) refreshLeaderIndex() {
	var newIndex int
	newIndex = ck.leaderIndex + 1
	if newIndex >= len(ck.servers) {
		newIndex = 0
	}
	ck.leaderIndex = newIndex
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	leaderIndex int
	clientId int64
	requestSeq int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderIndex = -1		// init leaderIndex
	ck.clientId = nrand()
	ck.requestSeq = 0
	return ck
}
