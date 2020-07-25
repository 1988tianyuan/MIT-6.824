package raftkv

import (
	"labrpc"
	"log"
	"time"
)

//func (ck *Clerk) Get(key string) string {
//	args := GetArgs{key}
//	success := false
//	var reply CommonReply
//	if ck.leaderIndex != -1 {
//		reply = CommonReply{}
//		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", &args, &reply)
//		success = ok && !reply.WrongLeader
//	}
//	for !success {
//		for index := range ck.servers {
//			reply = CommonReply{}
//			ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
//			log.Printf("Get: reply是：%v, 当前server是：%d, 发送的key是:%v", reply, index, key)
//			if ok && !reply.WrongLeader {
//				log.Printf("Get: 更新当前leaderIndex：%d", index)
//				ck.leaderIndex = index
//				success = true
//				break
//			}
//			time.Sleep(time.Duration(1) * time.Second)
//		}
//	}
//	if len(reply.Err) > 0 {
//		log.Printf("Get: failed to Get, args is %v, error is %v", args, reply.Err)
//	}
//	return reply.Content.(string)
//}
//
//func(ck *Clerk) PutAppend(key string, value string, op Operation) {
//	args := PutAppendArgs{key, value, op}
//	var reply CommonReply
//	if ck.leaderIndex != -1 {
//		reply = CommonReply{}
//		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", &args, &reply)
//		if ok && !reply.WrongLeader && len(reply.Err) <= 0  {
//			log.Printf("loopSendPutAppend: 发送成功，更新当前leaderIndex：%d, 发送的args是:%v", ck.leaderIndex,
//				args)
//			return
//		} else {
//			log.Printf("loopSendPutAppend: 发送失败，发送的args是:%v，收到的reply是:%v", args, reply)
//			if len(reply.Err) > 0 {
//				log.Printf("PutAppend: failed to putAppend, args is %v, error is %v", args, reply.Err)
//			}
//		}
//	}
//	success := false
//	for !success {
//		for index := range ck.servers {
//			reply = CommonReply{}
//			ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
//			if ok && !reply.WrongLeader && len(reply.Err) <= 0 {
//				log.Printf("loopSendPutAppend: 发送成功，更新当前leaderIndex：%d, 发送的args是:%v", index,
//					args)
//				ck.leaderIndex = index
//				success = true
//				break
//			} else {
//				log.Printf("loopSendPutAppend: 发送失败，发送的args是:%v，收到的reply是:%v", args, reply)
//				if len(reply.Err) > 0 {
//					log.Printf("PutAppend: failed to putAppend, args is %v, error is %v", args, reply.Err)
//				}
//			}
//			time.Sleep(time.Duration(1) * time.Second)
//		}
//	}
//}

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
		log.Printf("%v: 本次发送失败，发送的args是:%v，收到的reply是:%v, error是:%v, 是不是发到错误leader了:%v",
			rpcTarget, args, reply, reply.Err, reply.WrongLeader)
	} else {
		log.Printf("%v: 本次发送成功，发送的args是:%v，收到的reply是:%v",
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
