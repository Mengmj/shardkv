package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const PrintF = false

var flags = map[string]interface{}{
	"serverApply": nil,
	"trySnapshot": nil,
	//"test":         nil,
	//"PutAppend":    nil,
	//"KVServer.Get": nil,
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

type Record struct {
	ClerkId int64
	Seq     int64
	Result  string
}

type StateMachine struct {
	Mu           sync.Mutex
	Store        map[string]string
	Records      map[int64]Record
	AppliedIndex int
}

func (sm *StateMachine) execute(op *Op) {
	//sm.Mu.Lock()
	//defer sm.Mu.Unlock()
	if r, ok := sm.Records[op.ClerkId]; ok && r.Seq >= op.Seq {
		// 重复的kv操作,直接忽略
		return
	}
	var result string
	switch op.OpType {
	case GetOp:
		result = sm.Store[op.Key]
	case PutOp:
		sm.Store[op.Key] = op.Value
	case AppendOp:
		old := sm.Store[op.Key]
		var newValue string
		newValue = old + op.Value
		sm.Store[op.Key] = newValue
	}
	sm.Records[op.ClerkId] = Record{
		ClerkId: op.ClerkId,
		Seq:     op.Seq,
		Result:  result,
	}
}

// 查询状态机对于clerk最后一次执行kv操作seq及结果
func (sm *StateMachine) lastExecution(clerkId int64) (int64, string) {
	sm.Mu.Lock()
	defer sm.Mu.Unlock()
	if r, ok := sm.Records[clerkId]; ok { //已执行过该操作
		return r.Seq, r.Result
	} else {
		return -1, ""
	}
}

func (sm *StateMachine) lastApplied() int {
	sm.Mu.Lock()
	defer sm.Mu.Unlock()
	return sm.AppliedIndex
}

func (sm *StateMachine) retrieve(clerkId int64, seq int64) (string, bool) {
	sm.Mu.Lock()
	defer sm.Mu.Unlock()
	r, ok := sm.Records[clerkId]
	if ok && r.Seq == seq {
		return r.Result, true
	} else {
		return "", false
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
	persister    *raft.Persister
}

func (kv *KVServer) serverApply() {
	for msg := range kv.applyCh {
		kv.stateMachine.Mu.Lock()
		if msg.CommandValid { // 执行指令
			if msg.CommandIndex != kv.stateMachine.AppliedIndex+1 {
				FPrintf("serverApply", "server info:\n%v", kv.rf.Info())
				panic(fmt.Sprintf("StateMachine Execute Command Out Of Order Expect %v Got %v", kv.stateMachine.AppliedIndex+1, msg.CommandIndex))
			}
			op := msg.Command.(Op)
			kv.stateMachine.execute(&op)
			kv.stateMachine.AppliedIndex++
		} else if msg.SnapshotValid { // 恢复快照
			snapshot := msg.Snapshot
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			if d.Decode(&kv.stateMachine.Store) != nil {
				panic("read snapshot error")
			}
			if d.Decode(&kv.stateMachine.Records) != nil {
				panic("read snapshot error")
			}
			if d.Decode(&kv.stateMachine.AppliedIndex) != nil {
				panic("read snapshot error")
			}
			FPrintf("serverApply", "restore snapshot now state %v", kv.stateMachine)
		} else {
			panic(fmt.Sprintf("unexpected msg %v", msg))
		}
		kv.stateMachine.Mu.Unlock()
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
		reply.Value = result
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
				reply.Value = result
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

func (kv *KVServer) trySnapshot() {
	for !kv.killed() {
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.stateMachine.Mu.Lock()
			snapshotIndex := kv.stateMachine.AppliedIndex
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.stateMachine.Store)
			e.Encode(kv.stateMachine.Records)
			e.Encode(kv.stateMachine.AppliedIndex)
			snapshot := w.Bytes()
			kv.rf.Snapshot(snapshotIndex, snapshot)
			FPrintf("trySnapshot", "make snapshot for state %v", kv.stateMachine)
			kv.stateMachine.Mu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
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
		Store:   make(map[string]string),
		Records: make(map[int64]Record),
	}
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.serverApply()
	if maxraftstate > 0 {
		go kv.trySnapshot()
	}
	return kv
}
