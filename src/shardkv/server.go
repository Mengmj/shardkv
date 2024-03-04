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
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Shard  int
	Args   interface{}
}

type KVServer struct {
	mu           sync.RWMutex
	logger       *logger
	owner        string
	Status       int // 状态 0: 初始状态,无服务, 1:正常服务 2: 不接受新请求,待迁出 3:待迁入
	Shard        int // 负责的shard
	ConfigNum    int
	Store        map[string]string // 存储的内容
	AppliedSeq   map[int64]int64   // 各client的进度
	RepliedValue map[int64]string  // 给各clint的回复
}

//func (s *KVServer) commitIndex(){
//	commmitIndex(s.appliedIndex, s.IndexChan)
//}

//func commmitIndex(appliedIndex *int64, indexChan chan int64) {
//	for index := range indexChan {
//		for !atomic.CompareAndSwapInt64(appliedIndex, index-1, index) {
//			// 持续尝试更新index
//		}
//	}
//}

func (s *KVServer) getStatus() int {
	s.mu.RLock()
	s.mu.RUnlock()
	return s.Status
}

func (s *KVServer) getConfigNum() int {
	s.mu.RLock()
	s.mu.RUnlock()
	return s.ConfigNum
}

// 消费分派到shard的操作
func (s *KVServer) execute(op Op) {
	switch op.OpType {
	case GetOp:
		args := op.Args.(GetArgs)
		s.mu.Lock()
		if s.Status == 1 {
			if applied, ok := s.AppliedSeq[args.ClerkId]; ok && applied >= args.Seq {
				// 当前操作已执行过, 不再响应
			} else {
				s.RepliedValue[args.ClerkId] = s.Store[args.Key]
				s.AppliedSeq[args.ClerkId] = args.Seq
			}
		}
		s.mu.Unlock()
	case PutOp:
		args := op.Args.(PutAppendArgs)
		s.mu.Lock()
		if s.Status == 1 {
			if applied, ok := s.AppliedSeq[args.ClerkId]; ok && applied >= args.Seq {
				// 当前操作已执行过, 不再响应
			} else {
				s.Store[args.Key] = args.Value
				s.AppliedSeq[args.ClerkId] = args.Seq
			}
		}
		s.mu.Unlock()
	case AppendOp:
		args := op.Args.(PutAppendArgs)
		s.mu.Lock()
		if s.Status == 1 {
			if applied, ok := s.AppliedSeq[args.ClerkId]; ok && applied >= args.Seq {
				// 当前操作已执行过, 不再响应
			} else {
				s.Store[args.Key] = s.Store[args.Key] + args.Value
				s.AppliedSeq[args.ClerkId] = args.Seq
			}
		}
		s.mu.Unlock()
	case InstallShard:
		args := op.Args.(InstallShardArgs)
		s.mu.Lock()
		if s.Status == 3 {
			if s.ConfigNum == args.ConfigNum {
				s.Status = 1
				s.Store = args.Store
				s.AppliedSeq = args.AppliedSeq
				s.RepliedValue = args.RepliedValue
				s.logger.log("install shard %v into %v", s.Shard, s.owner)
			} else {
				s.logger.log("unmatched configNum server: %v, args: %v", s.ConfigNum, args.ConfigNum)
			}
		} else {
			s.logger.log("no need to install status %v", s.Status)
		}
		s.mu.Unlock()
	default:
		panic("unexpected msg")
	}
}

func MakeKVServer(gid int, peer int, shard int) *KVServer {
	server := &KVServer{
		owner:  fmt.Sprintf("%v-%v", gid, peer),
		Shard:  shard,
		logger: makeLogger(fmt.Sprintf("%v_%v_%v", gid, peer, shard)),
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
			e.Encode(kv.configNum)
			e.Encode(kv.configs)
			e.Encode(kv.appliedIndex)
			for i := 0; i < shardctrler.NShards; i++ {
				server := kv.shardServers[i]
				e.Encode(server.Status)
				e.Encode(server.ConfigNum)
				e.Encode(server.Store)
				e.Encode(server.AppliedSeq)
				e.Encode(server.RepliedValue)
			}
			kv.rf.Snapshot(int(kv.appliedIndex), w.Bytes())
			kv.logger.log("size after snapshot %v", kv.persister.RaftStateSize())
			kv.smMu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
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
				kv.rf.Start(op)
				kv.logger.log("submit new Config op %v", op)
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
	Shard        int
	ConfigNum    int
	Store        map[string]string
	AppliedSeq   map[int64]int64
	RepliedValue map[int64]string
}

type InstallShardReply struct {
	Success bool
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	kv.smMu.RLock()
	defer kv.smMu.RUnlock()
	shardServer := kv.shardServers[args.Shard]
	switch {
	case args.ConfigNum > shardServer.getConfigNum():
		reply.Success = false
	case args.ConfigNum == shardServer.getConfigNum():
		if shardServer.getStatus() == 3 {
			index, beginTerm, isLeader := kv.rf.Start(Op{
				OpType: InstallShard,
				Shard:  args.Shard,
				Args:   *args,
			})
			if !isLeader {
				reply.Success = false
			}
			for kv.getAppliedIndex() < int64(index) {
				if currentTerm, _ := kv.rf.GetState(); currentTerm != beginTerm {
					break
				}
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
			shardServer.mu.RLock()
			if shardServer.Status == 1 && shardServer.ConfigNum == args.ConfigNum {
				reply.Success = true
			}
			shardServer.mu.RUnlock()
		} else {
			reply.Success = true
		}
	case args.ConfigNum < shardServer.getConfigNum():
		// 已过时的分片
		reply.Success = true
	}

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
			Value = server.RepliedValue[clerkId]
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
			d.Decode(&kv.configNum)
			d.Decode(&kv.configs)
			d.Decode(&kv.appliedIndex)
			for i := 0; i < shardctrler.NShards; i++ {
				server := kv.shardServers[i]
				d.Decode(&server.Status)
				d.Decode(&server.ConfigNum)
				d.Decode(&server.Store)
				d.Decode(&server.AppliedSeq)
				d.Decode(&server.RepliedValue)
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
		moveOut := []int{}
		moveIn := []int{}
		newShard := []int{}
		for i := 0; i < len(newConfig.Shards); i++ {
			if lastConfig.Shards[i] == kv.gid {
				if newConfig.Shards[i] != kv.gid {
					moveOut = append(moveOut, i)
				}
			} else {
				if newConfig.Shards[i] == kv.gid {
					if lastConfig.Shards[i] == 0 {
						newShard = append(newShard, i)
					} else {
						moveIn = append(moveIn, i)
					}
				}
			}
		}
		for _, s := range newShard { //启动新shard的server
			server := kv.shardServers[s]
			server.mu.Lock()
			server.Status = 1
			server.ConfigNum = newConfig.Num
			server.Store = map[string]string{}
			server.AppliedSeq = map[int64]int64{}
			server.RepliedValue = map[int64]string{}
			server.mu.Unlock()
		}
		kv.logger.log("start new shard server")
		for _, s := range moveOut { // 标记为待迁出状态, 立即开始迁出, 不再处理之后的请求
			server := kv.shardServers[s]
			server.mu.Lock()
			server.Status = 2
			server.mu.Unlock()
			go kv.moveShard(s, newConfig)
		}
		kv.logger.log("start moveOut shard")
		for _, s := range moveIn {
			server := kv.shardServers[s]
			server.mu.Lock()
			server.Status = 3
			server.ConfigNum = newConfig.Num
			server.mu.Unlock()
		}
		kv.logger.log("set moveIn shard")
		kv.configs = append(kv.configs, newConfig)
		kv.configNum++
		kv.logger.log("updateConfig %v", newConfig)
	} else {
		kv.logger.log("expected configNum %v got %v", kv.configNum+1, newConfig.Num)
	}
}

func (kv *ShardKV) moveShard(shard int, cfg shardctrler.Config) {
	kv.smMu.RLock()
	defer kv.smMu.RUnlock()
	server := kv.shardServers[shard]
	GID := cfg.Shards[shard]
	ends := cfg.Groups[GID]
	receivers := []*labrpc.ClientEnd{}
	for _, end := range ends {
		receivers = append(receivers, kv.make_end(end))
	}
	kv.logger.log("try lock move shard %v from %v to %v", shard, kv.gid, GID)
	server.mu.RLock()
	args := InstallShardArgs{
		Shard:        server.Shard,
		ConfigNum:    cfg.Num,
		Store:        map[string]string{},
		AppliedSeq:   map[int64]int64{},
		RepliedValue: map[int64]string{},
	}
	for k, v := range server.Store {
		args.Store[k] = v
	}
	for c, seq := range server.AppliedSeq {
		args.AppliedSeq[c] = seq
	}
	for c, v := range server.RepliedValue {
		args.RepliedValue[c] = v
	}
	server.mu.RUnlock()

	finish := false
out:
	for kv.Killed() == 0 {
		for r := 0; r < len(receivers); r++ {
			reply := InstallShardReply{}
			kv.logger.log("try send move shard %v from %v to %v", shard, kv.gid, GID)
			ok := receivers[r].Call("ShardKV.InstallShard", &args, &reply)
			kv.logger.log("have sent move shard %v from %v to %v", shard, kv.gid, GID)
			if ok && reply.Success {
				finish = true
				break out
			}
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	server.mu.Lock()
	if finish {
		server.Store = map[string]string{}
		server.AppliedSeq = map[int64]int64{}
		server.RepliedValue = map[int64]string{}
		server.Status = 0
		kv.logger.log("move shard %v from %v to %v", shard, kv.gid, GID)
	}
	server.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
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
