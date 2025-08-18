package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID         int64 // requist ID
	CanRelease bool  // confirm received, release the cache

}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID         int64 // requist ID
	CanRelease bool  // confirm received, release the cache

}

type GetReply struct {
	Value string
}
