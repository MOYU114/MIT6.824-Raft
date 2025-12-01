package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int64
	leaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := &GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		CommandID: nrand(),
	}
	leaderID := ck.leaderID

	for {
		reply := &GetReply{}
		ok := ck.servers[leaderID].Call("KVServer.Get", args, reply)
		if !ok {
			// Wait a litte while, and change to another server
			DPrintf("%v client get key %v from server %v, not ok.", ck.clientID, key, leaderID)
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		} else if reply.Err != OK {
			DPrintf("%v client get key %v from server %v,reply err = %v!", ck.clientID, key, leaderID, reply.Err)
		}
		switch reply.Err {
		case OK:
			DPrintf("%v client get key %v from server %v,value: %v, OK.", ck.clientID, key, leaderID, reply.Value)
			ck.leaderID = leaderID
			return reply.Value
		case ErrNoKey:
			DPrintf("%v client get key %v from server %v, NO KEY!", ck.clientID, key, leaderID)
			ck.leaderID = leaderID
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		CommandID: nrand(),
	}
	leaderID := ck.leaderID

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[leaderID].Call("KVServer.PutAppend", args, reply)
		if !ok {
			// Wait a litte while, and change to another server
			DPrintf("%v client get key %v from server %v, not ok.", ck.clientID, key, leaderID)
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		} else if reply.Err != OK {
			DPrintf("%v client get key %v from server %v,reply err = %v!", ck.clientID, key, leaderID, reply.Err)
		}
		switch reply.Err {
		case OK:
			DPrintf("%v client get key %v from server %v,value: %v, OK.", ck.clientID, key, leaderID, value)
			ck.leaderID = leaderID
			return
		case ErrNoKey:
			DPrintf("%v client get key %v from server %v, NO KEY!", ck.clientID, key, leaderID)
			ck.leaderID = leaderID
			return
		case ErrTimeOut:
			continue
		case ErrWrongLeader:
			//换一个节点继续请求
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		case ErrServer:
			//换一个节点继续请求
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		default:
			log.Fatal("client rev unknown err", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
