package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

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
	Type  string
	Key   string
	Value string
	// duplicate detection info needs to be part of state machine
	// so that all raft servers eliminate the same duplicates
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
	// Maps Raft log index to channel that holds command committed to Raft Log at that index
	committedCmdCh map[int]chan Op
	// Maps clientId to requestId of command that was last applied to KV
	lastAppliedToKV map[int64]int64
}

//
// Get RPC handler
//
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, committedCmd := kv.commitCmdToLog(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = committedCmd.Value
}

//
// PutAppend RPC handler
//
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := kv.commitCmdToLog(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

//
// send the op log to Raft library and wait for it to be applied
//
func (kv *KVServer) commitCmdToLog(cmd Op) (bool, Op) {
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		return false, cmd
	}

	kv.mu.Lock()
	_, ok := kv.committedCmdCh[index]
	if !ok {
		kv.committedCmdCh[index] = make(chan Op, 1)
	}
	kv.mu.Unlock()

	select {
	case committedCmd := <-kv.committedCmdCh[index]:
		return kv.isSameCmd(cmd, committedCmd), committedCmd
	case <-time.After(600 * time.Millisecond):
		return false, cmd
	}
}

//
// check if the issued command is the same as the applied command
//
func (kv *KVServer) isSameCmd(issued Op, applied Op) bool {
	return issued.ClientId == applied.ClientId &&
		issued.RequestId == applied.RequestId
}

//
// background loop to receive the logs committed by the Raft
// library and apply them to the kv server state machine
//
func (kv *KVServer) run() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			cmd := msg.Command.(Op)
			kv.mu.Lock()
			kv.applyCmdToKV(&cmd, index)
			kv.pushCommittedCmdToCh(cmd, index)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {

		}
	}
}

func (kv *KVServer) applyCmdToKV(cmd *Op, index int) {
	if cmd.Type == "Get" {
		kv.applyToKV(cmd)
	} else {
		lastId, ok := kv.lastAppliedToKV[cmd.ClientId]
		if !ok || cmd.RequestId > lastId {
			kv.applyToKV(cmd)
			kv.lastAppliedToKV[cmd.ClientId] = cmd.RequestId
		}
	}
}

func (kv *KVServer) pushCommittedCmdToCh(cmd Op, index int) {
	_, ok := kv.committedCmdCh[index]
	if !ok {
		kv.committedCmdCh[index] = make(chan Op, 1)
	}
	kv.committedCmdCh[index] <- cmd
}

//
// applied the command to the state machine
// lock must be held before calling this
//
func (kv *KVServer) applyToKV(cmd *Op) {
	switch cmd.Type {
	case "Get":
		cmd.Value = kv.store[cmd.Key]
	case "Put":
		kv.store[cmd.Key] = cmd.Value
	case "Append":
		kv.store[cmd.Key] += cmd.Value
	}
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.committedCmdCh = make(map[int]chan Op)
	kv.lastAppliedToKV = make(map[int64]int64)

	// You may need initialization code here.
	go kv.run()

	return kv
}
