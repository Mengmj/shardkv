package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const PrintF = false

var flags = map[string]interface{}{
	"serverApply":  nil,
	"test":         nil,
	"PutAppend":    nil,
	"KVServer.Get": nil,
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func FPrintf(flag string, format string, a ...interface{}) {
	if _, ok := flags[flag]; ok && PrintF {
		log.Printf(flag+":::"+format, a...)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string
	Key     string
	Value   string
	ClerkId int64
	Seq     int64
}

type record struct {
	clerkId int64
	seq     int64
	result  *string
}

type StateMachine struct {
	mu           sync.Mutex
	store        map[string]*string
	records      map[int64]*record
	appliedIndex int
}

func (sm *StateMachine) execute(op *Op) {
	//sm.mu.Lock()
	//defer sm.mu.Unlock()
	if r, ok := sm.records[op.ClerkId]; ok && r.seq >= op.Seq {
		// 重复的kv操作,直接忽略
		return
	}
	var result *string
	switch op.OpType {
	case GetOp:
		result = sm.store[op.Key]
	case PutOp:
		sm.store[op.Key] = &op.Value
	case AppendOp:
		old := sm.store[op.Key]
		var newValue string
		if old == nil {
			newValue = op.Value
		} else {
			newValue = *old + op.Value
		}
		sm.store[op.Key] = &newValue
	}
	sm.records[op.ClerkId] = &record{
		clerkId: op.ClerkId,
		seq:     op.Seq,
		result:  result,
	}
}

// 查询状态机对于clerk最后一次执行kv操作seq及结果
func (sm *StateMachine) lastExecution(clerkId int64) (int64, *string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if r, ok := sm.records[clerkId]; ok { //已执行过该操作
		return r.seq, r.result
	} else {
		return -1, nil
	}
}

func (sm *StateMachine) lastApplied() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.appliedIndex
}

func (sm *StateMachine) retrieve(clerkId int64, seq int64) (*string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	r, ok := sm.records[clerkId]
	if ok && r.seq == seq {
		return r.result, true
	} else {
		return nil, false
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine StateMachine
}

func (kv *KVServer) serverApply() {
	for msg := range kv.applyCh {
		kv.stateMachine.mu.Lock()
		if msg.CommandValid { // 执行指令
			if msg.CommandIndex != kv.stateMachine.appliedIndex+1 {
				FPrintf("serverApply", "server info:\n%v", kv.rf.Info())
				panic(fmt.Sprintf("StateMachine Execute Command Out Of Order Expect %v Got %v", kv.stateMachine.appliedIndex+1, msg.CommandIndex))
			}
			op := msg.Command.(Op)
			kv.stateMachine.execute(&op)
			kv.stateMachine.appliedIndex++
		} else if msg.SnapshotValid { // 恢复快照
			// TODO 恢复快照
		} else {
			panic(fmt.Sprintf("unexpected msg %v", msg))
		}
		kv.stateMachine.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	executedSeq, result := kv.stateMachine.lastExecution(args.ClerkId)
	if executedSeq >= args.Seq {
		reply.Err = OK
		if result != nil {
			reply.Value = *result
		}
		return
	}
	op := Op{
		OpType:  GetOp,
		Key:     args.Key,
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
	}
	index, _, isLeader := kv.rf.Start(op)
	FPrintf("KVServer.Get", "server %v start with index of %v for op %v", kv.me, index, op)
	t0 := time.Now()
	for time.Since(t0).Milliseconds() < 2000 {
		if kv.stateMachine.lastApplied() >= index {
			result, ok := kv.stateMachine.retrieve(args.ClerkId, args.Seq)
			if !ok { // 当前server不再是leader,index对应的log未能提交已被覆盖
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK
				if result == nil {
					reply.Value = ""
				} else {
					reply.Value = *result
				}
				break
			}
		} else {
			time.Sleep(time.Duration(500) * time.Microsecond)
		}
	}
	if reply.Err != OK { // 该server已不是leader,未能达成一致
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	executedSeq, _ := kv.stateMachine.lastExecution(args.ClerkId)
	if executedSeq >= args.Seq {
		reply.Err = OK
		return
	}
	op := Op{
		OpType:  args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
	}
	index, _, isLeader := kv.rf.Start(op)
	t0 := time.Now()
	cnt := 0
	for time.Since(t0).Milliseconds() < 2000 {
		if kv.stateMachine.lastApplied() >= index {
			_, ok := kv.stateMachine.retrieve(args.ClerkId, args.Seq)
			if !ok { // 当前server不再是leader,index对应的log未能提交已被覆盖
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK
				break
			}
		} else {
			time.Sleep(time.Duration(1) * time.Millisecond)
			cnt++
		}
	}
	if reply.Err != OK { // 该server已不是leader,未能达成一致
		reply.Err = ErrWrongLeader
	}
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

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/result service.
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

	// You may need initialization code here.
	kv.stateMachine = StateMachine{
		store:   make(map[string]*string),
		records: make(map[int64]*record),
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.serverApply()

	return kv
}
