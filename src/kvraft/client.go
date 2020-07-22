package raftkv

import (
	"labrpc"
	"log"
	"time"
)

func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	return ck.doRPCRequest(&args, "KVServer.Get").(string)
}

func(ck *Clerk) PutAppend(key string, value string, op Operation) {
	args := PutAppendArgs{key, value, op}
	ck.doRPCRequest(&args, "KVServer.PutAppend")
}

func(ck *Clerk) doRPCRequest(args interface{}, rpcTarget string) interface{} {
	success, needRefreshLeader, content := ck.request(args, rpcTarget)
	if !success {
		if needRefreshLeader {
			ck.refreshLeaderIndex()
		}
		// sleep 50ms and retry
		time.Sleep(50 * time.Millisecond)
		return ck.doRPCRequest(args, rpcTarget)
	} else {
		return content
	}
}

func(ck *Clerk) request(args interface{}, rpcTarget string) (bool, bool, interface{}) {
	if ck.leaderIndex == -1 {
		return false, true, nil
	}
	reply := CommonReply{}
	ok := ck.servers[ck.leaderIndex].Call(rpcTarget, args, &reply)
	needRefreshLeader := !ok || reply.WrongLeader
	success := ok && !reply.WrongLeader && len(reply.Err) <= 0
	if !success {
		log.Printf("%v: 本次发送失败，发送的args是:%v，收到的reply是:%v, error是:%v, 是不是发到错误leader了:%v",
			rpcTarget, args, reply, reply.Err, reply.WrongLeader)
	} else {
		log.Printf("%v: 本次发送成功，发送的args是:%v，收到的reply是:%v",
			rpcTarget, args, reply)
	}
	return success, needRefreshLeader, reply.Content
}

func(ck *Clerk) refreshLeaderIndex() {
	var newIndex int
	curLeader := ck.leaderIndex
	total := len(ck.servers)
	if curLeader == -1 {
		newIndex = 0
	} else {
		newIndex = curLeader + 1
		if newIndex >= total {
			newIndex = 0
		}
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
	// You will have to modify this struct.
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderIndex = -1		// init leaderIndex
	// You'll have to add code here.
	return ck
}
