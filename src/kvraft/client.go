package raftkv

import (
	"labrpc"
	"log"
	"time"
)
import cryptoRand "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	leaderIndex int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := cryptoRand.Int(cryptoRand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderIndex = -1		// init leaderIndex
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	success := false
	var reply GetReply
	if ck.leaderIndex != -1 {
		reply = GetReply{}
		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", &args, &reply)
		success = ok && !reply.WrongLeader
	}
	for !success {
		for index := range ck.servers {
			reply = GetReply{}
			ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
			log.Printf("Get: reply是：%v, 当前server是：%d, 发送的key是:%v", reply, index, key)
			if ok && !reply.WrongLeader {
				log.Printf("Get: 更新当前leaderIndex：%d", index)
				ck.leaderIndex = index
				success = true
				break
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
	if len(reply.Err) > 0 {
		log.Printf("Get: failed to Get, args is %v, error is %v", args, reply.Err)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func(ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op}
	var reply PutAppendReply
	if ck.leaderIndex != -1 {
		reply = PutAppendReply{}
		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader && len(reply.Err) <= 0  {
			log.Printf("loopSendPutAppend: 发送成功，更新当前leaderIndex：%d, 发送的args是:%v", ck.leaderIndex,
				args)
			return
		} else {
			log.Printf("loopSendPutAppend: 发送失败，发送的args是:%v，收到的reply是:%v", args, reply)
			if len(reply.Err) > 0 {
				log.Printf("PutAppend: failed to putAppend, args is %v, error is %v", args, reply.Err)
			}
		}
	}
	success := false
	for !success {
		for index := range ck.servers {
			reply = PutAppendReply{}
			ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
			if ok && !reply.WrongLeader && len(reply.Err) <= 0 {
				log.Printf("loopSendPutAppend: 发送成功，更新当前leaderIndex：%d, 发送的args是:%v", index,
					args)
				ck.leaderIndex = index
				success = true
				break
			} else {
				log.Printf("loopSendPutAppend: 发送失败，发送的args是:%v，收到的reply是:%v", args, reply)
				if len(reply.Err) > 0 {
					log.Printf("PutAppend: failed to putAppend, args is %v, error is %v", args, reply.Err)
				}
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
