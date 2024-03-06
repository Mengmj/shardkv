package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const (
	NewConfig    = "NewConfig"
	GetOp        = "GetOP"
	PutOp        = "PutOP"
	AppendOp     = "AppendOp"
	InstallShard = "InstallShard"
	RemoveShard  = "RemoveShard"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Shard  int
	Args   interface{}
}

const (
	Idle = iota
	Working
	In

	Ready
	WaitData
)

type KVServer struct {
	mu          sync.RWMutex
	logger      *logger
	owner       string
	Status      int
	OutShards   []*InstallShardArgs // 要迁出的数据副本, 数据可能未就绪(等待迁入然后才能迁出)
	InConfigNum int                 //导致shard迁入的版本号
	MoveInNums  map[int]interface{} //成功迁入的版本号
	// 以下是需要迁移的字段
	Shard        int                // 负责的shard
	Store        map[string]*string // 存储的内容
	AppliedSeq   map[int64]int64    // 各client的进度
	RepliedValue map[int64]*string  // 给各clint的回复
}

func (s *KVServer) getStatus() int {
	s.mu.RLock()
	s.mu.RUnlock()
	return s.Status
}

func (s *KVServer) getConfigNum() int {
	s.mu.RLock()
	s.mu.RUnlock()
	return s.InConfigNum
}

func (s *KVServer) installShard(args InstallShardArgs) {
	if _, ok := s.MoveInNums[args.OutConfigNum]; ok {
		s.logger.log("shard of configNum %v already moved in", args.OutConfigNum)
		return
	}
	switch {
	case args.OutConfigNum == s.InConfigNum:
		if s.Status == In {
			s.Status = Working
			s.Store = args.Store
			s.AppliedSeq = args.AppliedSeq
			s.RepliedValue = args.RepliedValue
			s.MoveInNums[args.OutConfigNum] = nil
			s.logger.log("install shard %v for working InConfigNum:%v", s.Shard, s.InConfigNum)
		}
	case args.OutConfigNum < s.InConfigNum || s.InConfigNum == 0:
		for _, outShard := range s.OutShards {
			if outShard.InConfigNum == args.OutConfigNum && outShard.Status == WaitData {
				outShard.Store = args.Store
				outShard.AppliedSeq = args.AppliedSeq
				outShard.RepliedValue = args.RepliedValue
				outShard.Status = Ready
				outShard.mu.Unlock()
				s.MoveInNums[args.OutConfigNum] = nil
				s.logger.log("install shard %v for out OutConfigNum:%v", s.Shard, outShard.OutConfigNum)
			}
		}
	}
}

// 消费分派到shard的操作
func (s *KVServer) execute(op Op) {
	s.logger.log("start op %v", op)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op.OpType {
	case GetOp:
		args := op.Args.(GetArgs)
		if s.Status == Working {
			if applied, ok := s.AppliedSeq[args.ClerkId]; ok && applied >= args.Seq {
				// 当前操作已执行过, 不再响应
			} else {
				s.RepliedValue[args.ClerkId] = s.Store[args.Key]
				s.AppliedSeq[args.ClerkId] = args.Seq
				s.logger.log("get %v %v after execution", args.Key, s.RepliedValue[args.ClerkId])
			}
		} else {
			s.logger.log("not serving for shard %v", s.Shard)
		}
	case PutOp:
		args := op.Args.(PutAppendArgs)
		if s.Status == Working {
			if applied, ok := s.AppliedSeq[args.ClerkId]; ok && applied >= args.Seq {
				// 当前操作已执行过, 不再响应
			} else {
				s.Store[args.Key] = &args.Value
				s.AppliedSeq[args.ClerkId] = args.Seq
				s.logger.log("put %v %v after execution", args.Key, args.Value)
			}
		} else {
			s.logger.log("not serving for shard %v", s.Shard)
		}
	case AppendOp:
		args := op.Args.(PutAppendArgs)
		if s.Status == Working {
			if applied, ok := s.AppliedSeq[args.ClerkId]; ok && applied >= args.Seq {
				// 当前操作已执行过, 不再响应
			} else {
				newValue := *s.Store[args.Key] + args.Value
				s.Store[args.Key] = &newValue
				s.AppliedSeq[args.ClerkId] = args.Seq
				s.logger.log("append %v %v after execution", args.Key, args.Value)
			}
		} else {
			s.logger.log("not serving for shard %v", s.Shard)
		}
	case InstallShard:
		s.installShard(op.Args.(InstallShardArgs))
	case RemoveShard:
		outConfigNum := op.Args.(int)
		for i, outShard := range s.OutShards {
			if outShard.OutConfigNum == outConfigNum {
				s.OutShards = append(s.OutShards[:i], s.OutShards[i+1:]...)
				s.logger.log("remove shard %v OutConfigNum %v after moving", s.Shard, outConfigNum)
			}
		}
	default:
		panic("unexpected msg")
	}
	s.logger.log("finish op %v", op)
}

func MakeKVServer(gid int, peer int, shard int) *KVServer {
	server := &KVServer{
		owner:      fmt.Sprintf("%v-%v", gid, peer),
		Shard:      shard,
		logger:     makeLogger(fmt.Sprintf("%v_%v_%v", gid, peer, shard)),
		MoveInNums: map[int]interface{}{},
	}
	return server
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead      int32
	ctlClerk  *shardctrler.Clerk
	persister *raft.Persister
	logger    *logger

	// 状态机内容
	smMu    sync.RWMutex
	cfgMu   sync.RWMutex
	indexMu sync.RWMutex

	shardServers [shardctrler.NShards]*KVServer
	configNum    int                  // 最新的config版本号
	configs      []shardctrler.Config // config的历史版本
	appliedIndex int64
}

func (kv *ShardKV) getConfigNum() int {
	kv.cfgMu.RLock()
	defer kv.cfgMu.RUnlock()
	return kv.configNum
}

//func (kv *ShardKV) commitIndex() {
//	commmitIndex(&kv.appliedIndex, kv.indexChan)
//}

func (kv *ShardKV) makeSnapshot() {
	for kv.Killed() == 0 {
		if kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.logger.log("size before snapshot %v", kv.persister.RaftStateSize())
			kv.logger.log("try lock to make snapshot")
			kv.smMu.Lock() // 这里需要一个排他锁
			kv.logger.log("get lock to make snapshot")
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			if e.Encode(kv.configNum) != nil || e.Encode(kv.configs) != nil || e.Encode(kv.appliedIndex) != nil {
				panic("encode error")
			}
			for i := 0; i < shardctrler.NShards; i++ {
				server := kv.shardServers[i]
				panicIfEncodeErr(e, server.Status)
				panicIfEncodeErr(e, server.InConfigNum)
				panicIfEncodeErr(e, server.Store)
				panicIfEncodeErr(e, server.AppliedSeq)
				panicIfEncodeErr(e, server.RepliedValue)
				panicIfEncodeErr(e, server.MoveInNums)
				panicIfEncodeErr(e, len(server.OutShards))
				for _, outShard := range server.OutShards {
					kv.logger.log("include out Shard in snapshot")
					panicIfEncodeErr(e, outShard)
				}
			}
			kv.rf.Snapshot(int(kv.appliedIndex), w.Bytes())
			kv.logger.log("size after snapshot %v", kv.persister.RaftStateSize())
			kv.smMu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func panicIfEncodeErr(e *labgob.LabEncoder, i interface{}) {
	if e.Encode(i) != nil {
		panic("encode error")
	}
	fmt.Printf("length is %v (%v)\n", encodeLength(i), i)
}

func panicIfDecodeErr(d *labgob.LabDecoder, i interface{}) {
	if d.Decode(i) != nil {
		panic("decode error")
	}
}

func (kv *ShardKV) queryConfig() {
	for kv.Killed() == 0 {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			oldConfigNum := kv.getConfigNum()
			newConfig := kv.ctlClerk.Query(oldConfigNum + 1)
			if newConfig.Num == oldConfigNum+1 {
				kv.logger.log("get new config %v, old config %v", newConfig.Num, oldConfigNum)
				op := Op{
					OpType: NewConfig,
					Shard:  -1,
					Args:   newConfig,
				}
				_, beginTerm, _ := kv.rf.Start(op)
				kv.logger.log("start commit new Config %v", newConfig)
				// 等待状态机更新config或失败(term变更)
				fail := false
				for kv.getConfigNum() < newConfig.Num {
					if currentTerm, _ := kv.rf.GetState(); currentTerm != beginTerm {
						fail = true
						break
					}
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
				// 成功后检查是否需要迁移shard
				if !fail {
					kv.logger.log("success to apply new Config %v", newConfig.Num)
					for i := 0; i < shardctrler.NShards; i++ {
						kv.shardServers[i].mu.RLock()
						for _, outArgs := range kv.shardServers[i].OutShards {
							go kv.moveShard(outArgs)
						}
						kv.shardServers[i].mu.RUnlock()
					}
				}
			}
		}
		ms := 50 + nrand()%100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err, reply.Value = kv.wait(args.ClerkId, args.Seq, Op{
		OpType: GetOp,
		Shard:  args.Shard,
		Args:   *args,
	})
	if reply.Err == OK && reply.Value == "" {
		kv.logger.log("return empty")
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err, _ = kv.wait(args.ClerkId, args.Seq, Op{
		OpType: args.Op,
		Shard:  args.Shard,
		Args:   *args,
	})
}

type InstallShardArgs struct {
	mu           sync.Mutex
	Shard        int
	Ends         []string
	InConfigNum  int // 在原宿主迁入的版本号
	OutConfigNum int // 导致迁出的版本号
	Status       int
	Store        map[string]*string
	AppliedSeq   map[int64]int64
	RepliedValue map[int64]*string
}

type InstallShardReply struct {
	Success bool
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	//kv.smMu.RLock()
	//defer kv.smMu.RUnlock()
	//shardServer := kv.shardServers[args.Shard]
	//InConfigNum := shardServer.getConfigNum()
	index, beginTerm, isLeader := kv.rf.Start(Op{
		OpType: InstallShard,
		Shard:  args.Shard,
		Args:   *args,
	})
	if !isLeader {
		reply.Success = false
		return
	}
	kv.logger.log("start to commit install shard %v", args.Shard)
	for kv.getAppliedIndex() < int64(index) {
		if currentTerm, _ := kv.rf.GetState(); currentTerm != beginTerm {
			reply.Success = false
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	reply.Success = kv.HasMovedIn(args.Shard, args.OutConfigNum)
	// 即使通过raft达成一致, shard也不一定成功迁入
	if reply.Success {
		kv.logger.log("agree to install shard %v", args.Shard)
	}

}

func (kv *ShardKV) HasMovedIn(shard int, outConfigNum int) bool {
	server := kv.shardServers[shard]
	server.mu.RLock()
	defer server.mu.RUnlock()
	_, ok := server.MoveInNums[outConfigNum]
	return ok
}

func (kv *ShardKV) getAppliedIndex() int64 {
	kv.indexMu.RLock()
	defer kv.indexMu.RUnlock()
	return kv.appliedIndex
}

// 等待命令执行完毕或出错
func (kv *ShardKV) wait(clerkId int64, seq int64, command Op) (Err Err, Value string) {
	index, beginTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		Err = ErrWrongLeader
		return
	}
	cnt := 1
	for kv.getAppliedIndex() < int64(index) {
		currentTerm, _ := kv.rf.GetState()
		if currentTerm != beginTerm {
			Err = ErrWrongLeader
			return
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
		cnt++
		if cnt%100 == 0 {
			kv.logger.log("wait too long for index:%v clerkId:%v seq:%v", index, clerkId, seq)
		}
	}

	if currentTerm, _ := kv.rf.GetState(); currentTerm == beginTerm {
		server := kv.shardServers[command.Shard]
		server.mu.RLock()
		if server.AppliedSeq[clerkId] >= seq {
			Err = OK
			valuePtr := server.RepliedValue[clerkId]
			if valuePtr != nil {
				Value = *valuePtr
			}
		} else { // 达成共识却没有执行,因为分片已迁出
			Err = ErrWrongGroup
		}
		server.mu.RUnlock()
	} else {
		Err = ErrWrongLeader
	}
	return
}

// 消费applyCh中的msg,将其应用于状态机
func (kv *ShardKV) consume() {
	for msg := range kv.applyCh {
		kv.logger.log("receive %v", msg)
		kv.smMu.RLock()       // 此处需要共享锁
		if msg.CommandValid { // 执行指令
			op := msg.Command.(Op)
			if op.Shard != -1 { // 可以分派到shard的指令
				kv.shardServers[op.Shard].execute(op)
			} else { // 不可分配的命令: 更新配置, 将该指令分解到各shard
				kv.updateConfig(op.Args.(shardctrler.Config))
			}
			kv.indexMu.Lock()
			if int64(msg.CommandIndex) != kv.appliedIndex+1 {
				panic(fmt.Sprintf("state machine receive msg out of order expect %v, got %v", kv.appliedIndex+1, msg.CommandIndex))
			} else {
				kv.appliedIndex++
			}
			kv.indexMu.Unlock()
		} else if msg.SnapshotValid {
			// TODO 恢复快照
			snapshot := msg.Snapshot
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			kv.cfgMu.Lock()
			kv.indexMu.Lock()
			for i := 0; i < shardctrler.NShards; i++ {
				kv.shardServers[i].mu.Lock()
			}
			panicIfDecodeErr(d, &kv.configNum)
			panicIfDecodeErr(d, &kv.configs)
			panicIfDecodeErr(d, &kv.appliedIndex)
			for i := 0; i < shardctrler.NShards; i++ {
				server := kv.shardServers[i]
				panicIfDecodeErr(d, &server.Status)
				if server.Status != Idle && server.Status != Working && server.Status != In {
					kv.logger.log("")
				}
				panicIfDecodeErr(d, &server.InConfigNum)
				panicIfDecodeErr(d, &server.Store)
				panicIfDecodeErr(d, &server.AppliedSeq)
				panicIfDecodeErr(d, &server.RepliedValue)
				panicIfDecodeErr(d, &server.MoveInNums)
				server.OutShards = []*InstallShardArgs{}
				var outCount int
				panicIfDecodeErr(d, &outCount)
				for j := 0; j < outCount; j++ {
					outData := InstallShardArgs{}
					panicIfDecodeErr(d, &outData)
					if outData.Status == WaitData {
						outData.mu.Lock()
					}
					server.OutShards = append(server.OutShards, &outData)
				}
			}
			for i := 0; i < shardctrler.NShards; i++ {
				kv.shardServers[i].mu.Unlock()
			}
			kv.cfgMu.Unlock()
			kv.indexMu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected msg %v", msg))
		}
		kv.smMu.RUnlock()
		kv.logger.log("consume %v", msg)
	}
}

func (kv *ShardKV) updateConfig(newConfig shardctrler.Config) {
	kv.cfgMu.Lock()
	defer kv.cfgMu.Unlock()
	if newConfig.Num == kv.configNum+1 { // 只接受连续的版本
		lastConfig := kv.configs[len(kv.configs)-1]
		kv.configs = append(kv.configs, newConfig)
		kv.configNum++
		var change int // 0 不变, 1初始 2迁入, 3迁出
		const (
			NoChange = 0
			NewShard = 1
			MoveIn   = 2
			MoveOut  = 3
		)
		for i := 0; i < len(newConfig.Shards); i++ {
			if lastConfig.Shards[i] == kv.gid {
				if newConfig.Shards[i] != kv.gid {
					change = MoveOut
				} else {
					change = NoChange
				}
			} else {
				if newConfig.Shards[i] == kv.gid {
					if lastConfig.Shards[i] == 0 {
						change = NewShard
					} else {
						change = MoveIn
					}
				} else {
					change = NoChange
				}
			}
			server := kv.shardServers[i]
			server.mu.Lock()
			switch change {
			case NewShard:
				if server.Status != Idle {
					panic("shard rewritten")
				}
				server.Status = Working
				server.InConfigNum = newConfig.Num
				server.Store = map[string]*string{}
				server.AppliedSeq = map[int64]int64{}
				server.RepliedValue = map[int64]*string{}
				kv.logger.log("allocated a new shard %v", i)
			case MoveIn:
				if server.Status != Idle {
					panic("shard rewritten")
				}
				server.Status = In
				server.InConfigNum = newConfig.Num
				kv.logger.log("need to move In shard %v", i)
			case MoveOut:
				target := newConfig.Shards[i]
				outArgs := &InstallShardArgs{
					Shard:        server.Shard,
					Ends:         newConfig.Groups[target],
					InConfigNum:  server.InConfigNum,
					OutConfigNum: newConfig.Num,
					Store:        server.Store,
					AppliedSeq:   server.AppliedSeq,
					RepliedValue: server.RepliedValue,
				}
				server.OutShards = append(server.OutShards, outArgs)
				server.Store = nil
				server.RepliedValue = nil
				server.AppliedSeq = nil
				if server.Status == Working {
					outArgs.Status = Ready
				} else if server.Status == In {
					outArgs.Status = WaitData
					outArgs.mu.Lock()
				} else {
					panic("Wrong Status")
				}
				server.Status = Idle
				server.InConfigNum = 0
				kv.logger.log("need to move out shard %v", i)
			}
			server.mu.Unlock()
		}
		kv.logger.log("updateConfig %v", newConfig)
	} else {
		kv.logger.log("expected configNum %v got %v", kv.configNum+1, newConfig.Num)
	}
}

// 拿到副本进行迁移
func (kv *ShardKV) moveShard(args *InstallShardArgs) {
	// 尝试获取锁,确保数据已就绪
	args.mu.Lock()
	args.mu.Unlock()
	kv.logger.log("start to move shard %v(outConfigNum:%v) to %v", args.Shard, args.OutConfigNum, args.Ends)
	receivers := []*labrpc.ClientEnd{}
	for _, end := range args.Ends {
		receivers = append(receivers, kv.make_end(end))
	}
	success := false
out:
	for kv.Killed() == 0 {
		for r := 0; r < len(receivers); r++ {
			reply := InstallShardReply{}
			ok := receivers[r].Call("ShardKV.InstallShard", args, &reply)
			if ok && reply.Success {
				success = true
				break out
			}
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	if success {
		kv.rf.Start(Op{
			OpType: RemoveShard,
			Shard:  args.Shard,
			Args:   args.OutConfigNum,
		}) // 不必等待完成,大不了重发
		kv.logger.log("success to move shard %v(outConfigNum:%v) to %v", args.Shard, args.OutConfigNum, args.Ends)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.logger.log("killed")
}
func (kv *ShardKV) Killed() int32 {
	return atomic.LoadInt32(&kv.dead)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(InstallShardArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.logger = makeLogger(fmt.Sprintf("%v_%v", kv.gid, kv.me))
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.ctlClerk = shardctrler.MakeClerk(ctrlers)
	kv.configs = []shardctrler.Config{{}}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardServers[i] = MakeKVServer(gid, me, i)
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.queryConfig()
	go kv.consume()
	if maxraftstate != -1 {
		go kv.makeSnapshot()
	}
	return kv
}

func encodeLength(item interface{}) int {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(item)
	return len(w.Bytes())
}

//func timingLock(lock *sync.Mutex) {
//	success := false
//	for i := 0; i < 10000; i++ {
//		if lock.TryLock() {
//			success = true
//			break
//		}
//	}
//	if !success {
//		FPrint("timingLock", "wait too long")
//	}
//}
//
//func timingRWLock(lock *sync.RWMutex) {
//	success := false
//	for i := 0; i < 10000; i++ {
//		if lock.TryLock() {
//			success = true
//			break
//		}
//	}
//	if !success {
//		FPrint("timingLock", "wait too long")
//	}
//}

//func (kv *ShardKV) FPrint(flag string, format string, a ...interface{}) {
//	if _, ok := flags[flag]; ok && PringF {
//		a = append([]interface{}{kv.gid, kv.me}, a...)
//		msg := fmt.Sprintf(format, a...)
//		fmt.Printf("%v-%v,%v,%v\n", kv.gid, kv.me, flag, msg)
//	}
//}
