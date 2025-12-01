package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

const WaitCmdTimeOut = time.Millisecond * 500
const MaxLockTime = time.Millisecond * 10

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqID     int64 // for commandNotify
	CommandID int64
	ClientID  int64
	Key       string
	Value     string
	Method    string
}
type CommandResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandNotifyCh map[int64]chan CommandResult
	lastApplies     map[int64]int64 //k-v：ClientId-CommandId
	data            map[string]string

	//ch
	stopCh chan struct{}
	// persister
	persister *raft.Persister

	// lock
	lockStartTime time.Time
	lockEndTime   time.Time
	lockMsg       string
}

// lock def
func (kv *KVServer) lock(msg string) {
	kv.mu.Lock()
	kv.lockStartTime = time.Now()
	kv.lockMsg = msg
}

func (kv *KVServer) unlock(msg string) {
	kv.lockEndTime = time.Now()
	duration := kv.lockEndTime.Sub(kv.lockStartTime)
	kv.lockMsg = ""
	kv.mu.Unlock()
	if duration > MaxLockTime {
		DPrintf("lock too long:%s:%s\n", msg, duration)
	}
}

func (kv *KVServer) removeCh(reqId int64) {
	kv.lock("removeCh")
	defer kv.unlock("removeCh")
	delete(kv.commandNotifyCh, reqId)
}

// use start to get cmd from Raft
func (kv *KVServer) waitCmd(op Op) (res CommandResult) {
	DPrintf("server %v wait cmd start,Op: %+v.\n", kv.me, op)
	index, term, isleader := kv.rf.Start(op)

	if !isleader {
		// return when its not leader
		res.Err = ErrWrongLeader
		return
	}
	kv.lock("waitCmd")
	ch := make(chan CommandResult, 1)
	kv.commandNotifyCh[op.ReqID] = ch
	kv.unlock("waitCmd")
	DPrintf("start cmd: index:%d, term:%d, op:%+v", index, term, op)

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case <-kv.stopCh:
		DPrintf("stop ch waitCmd")
		kv.removeCh(op.ReqID)
		res.Err = ErrServer
		return
	case res = <-ch:
		kv.removeCh(op.ReqID)
		return
	case <-t.C:
		kv.removeCh(op.ReqID)
		res.Err = ErrTimeOut
		return
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server %v in rpc Get,args: %+v", kv.me, args)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		ReqID:     nrand(),
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
		Key:       args.Key,
		Method:    "Get",
	}
	// wait for cmd to operate
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value

	DPrintf("server %v in rpc Get,args: %+v,reply: %+v", kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server %v in rpc PutAppend,args: %+v", kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var value string
	if args.Op == "Put" {
		value = args.Value
	} else if args.Op == "Append" {
		value = args.Value // The actual append logic is handled in handleApplyCh
	}

	op := Op{
		ReqID:     nrand(),
		ClientID:  args.ClientID,
		CommandID: args.CommandID,
		Key:       args.Key,
		Value:     value,
		Method:    args.Op,
	}

	res := kv.waitCmd(op)
	reply.Err = res.Err

	DPrintf("server %v in rpc PutAppend,args:%+v,reply:%+v", kv.me, args, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// snapshot
func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	//生成快照数据
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastApplies); err != nil {
		panic(err)
	}
	data := w.Bytes()
	kv.rf.Snapshot(logIndex, data)
}

// read snapshot
func (kv *KVServer) readPersist(isInit bool, snapshotTerm, snapshotIndex int, data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	if !isInit {
		args := &raft.InstallSnapshotArgs{
			Term:              snapshotTerm,
			LeaderID:          kv.me,
			LastIncludedIndex: snapshotIndex,
			LastIncludedTerm:  snapshotTerm,
			Data:              data,
		}
		reply := &raft.InstallSnapshotReply{}
		kv.rf.InstallSnapshot(args, reply)
		if reply.Term < 0 {
			log.Panicln("kv read persist err in CondInstallSnapshot!")
			return
		}
	}
	// sync
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var lastApplies map[int64]int64

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil {
		log.Fatal("kv read persist err!")
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
	}
}

func (kv *KVServer) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.data[key]; ok {
		err = OK
		value = v
	} else {
		err = ErrNoKey
	}
	return
}

func (kv *KVServer) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

// After send the data to the Raft, need to log the result in the server.
func (kv *KVServer) handleApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			DPrintf("get from stopCh,server-%v stop!", kv.me)
			return
		case cmd := <-kv.applyCh:
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				DPrintf("%v get install sn,%v %v", kv.me, cmd.SnapshotIndex, cmd.SnapshotTerm)
				kv.lock("waitApplyCh_sn")
				kv.readPersist(false, cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
				kv.unlock("waitApplyCh_sn")
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			DPrintf("server %v start apply command %v: %+v", kv.me, cmdIdx, cmd.Command)
			op := cmd.Command.(Op)
			kv.lock("handleApplyCh")

			if op.Method == "Get" {
				//处理读
				e, v := kv.getValueByKey(op.Key)
				kv.notifyWaitCommand(op.ReqID, e, v)
			} else if op.Method == "Put" || op.Method == "Append" {
				//处理写
				//判断命令是否重复
				isRepeated := false
				if v, ok := kv.lastApplies[op.ClientID]; ok {
					if v == op.CommandID {
						isRepeated = true
					}
				}

				if !isRepeated {
					switch op.Method {
					case "Put":
						kv.data[op.Key] = op.Value
						kv.lastApplies[op.ClientID] = op.CommandID
					case "Append":
						e, v := kv.getValueByKey(op.Key)
						if e == ErrNoKey {
							//按put处理
							kv.data[op.Key] = op.Value
							kv.lastApplies[op.ClientID] = op.CommandID
						} else {
							//追加
							kv.data[op.Key] = v + op.Value
							kv.lastApplies[op.ClientID] = op.CommandID
						}
					default:
						kv.unlock("handleApplyCh")
						panic("unknown method " + op.Method)
					}

				}
				//命令处理成功
				kv.notifyWaitCommand(op.ReqID, OK, "")
			} else {
				kv.unlock("handleApplyCh")
				panic("unknown method " + op.Method)
			}

			DPrintf("apply op: cmdId:%d, op: %+v, data:%v", cmdIdx, op, kv.data[op.Key])
			//每应用一条命令，就判断是否进行持久化
			kv.saveSnapshot(cmdIdx)

			kv.unlock("handleApplyCh")
		}

	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.lastApplies = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.stopCh = make(chan struct{})
	//读取快照
	kv.readPersist(true, 0, 0, kv.persister.ReadSnapshot())
	kv.commandNotifyCh = make(map[int64]chan CommandResult)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.handleApplyCh()

	return kv
}
