package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	KVMap   map[string]string // key-value store
	KVCache map[int64]string  // cache for request IDs

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.CanRelease {
		if _, ok := kv.KVCache[args.ID]; ok { // release the cache
			delete(kv.KVCache, args.ID)
		}
		return
	}
	if value, ok := kv.KVCache[args.ID]; ok {
		reply.Value = value
	} else {
		val := kv.KVMap[args.Key]
		reply.Value = val
		kv.KVCache[args.ID] = val // store the value for this operation
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.CanRelease {
		if _, ok := kv.KVCache[args.ID]; ok { // release the cache
			delete(kv.KVCache, args.ID)
		}
		return
	}

	if _, ok := kv.KVCache[args.ID]; ok {
		// if the request ID already exists, means already done it, return the cached value
		reply.Value = kv.KVCache[args.ID]
	} else {
		// otherwise, do the put operation, return old value
		oldValue := kv.KVMap[args.Key]
		newValue := args.Value
		kv.KVMap[args.Key] = newValue
		reply.Value = oldValue
		kv.KVCache[args.ID] = oldValue // store the value for this operation
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.CanRelease {
		if _, ok := kv.KVCache[args.ID]; ok { // release the cache
			delete(kv.KVCache, args.ID)
		}
		return
	}

	if _, ok := kv.KVCache[args.ID]; ok {
		// if the request ID already exists, means already done it, return the cached value
		reply.Value = kv.KVCache[args.ID]
	} else {
		// otherwise, do the append operation, return old value
		oldValue := kv.KVMap[args.Key]
		newValue := oldValue + args.Value
		kv.KVMap[args.Key] = newValue
		reply.Value = oldValue
		kv.KVCache[args.ID] = oldValue // store the value for this operation
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.KVMap = make(map[string]string)  // initialize the key-value store
	kv.KVCache = make(map[int64]string) // initialize the cache for request IDs

	// You may need initialization code here.

	return kv
}
