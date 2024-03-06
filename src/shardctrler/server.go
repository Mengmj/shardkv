package shardctrler

import (
	"6.5840/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32

	smMu sync.Mutex
	// 状态机内容
	configs          []Config // indexed by config num
	appliedIndex     int
	AppliedSeq       map[int64]int64 // 对各clerk回复的最大seq
	ReplyConfigIndex map[int64]int   // 对各clerk回复的config index
	allocation       map[int][]int   //
}

type OpString string

const (
	JoinOp  = "join"
	MoveOp  = "move"
	LeaveOp = "leave"
	QueryOp = "query"
)

type Op struct {
	// Your data here.
	OpType OpString
	Args   interface{} //for leave
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{
		OpType: JoinOp,
		Args:   *args,
	}
	reply.WrongLeader, reply.Err, _ = sc.wait(args.ClerkId, args.Seq, command)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{
		OpType: LeaveOp,
		Args:   *args,
	}
	reply.WrongLeader, reply.Err, _ = sc.wait(args.ClerkId, args.Seq, command)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{
		OpType: MoveOp,
		Args:   *args,
	}
	reply.WrongLeader, reply.Err, _ = sc.wait(args.ClerkId, args.Seq, command)
}

// 等待command被执行
func (sc *ShardCtrler) wait(clerkId int64, seq int64, command Op) (WrongLeader bool, Err Err, Cfg Config) {
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		WrongLeader = true
		return
	}
	begin := time.Now()
	var wait bool
	for time.Since(begin).Milliseconds() < 2000 {
		sc.smMu.Lock()
		if sc.appliedIndex >= index {
			if sc.AppliedSeq[clerkId] >= seq {
				Err = OK
				Cfg = sc.configs[sc.ReplyConfigIndex[clerkId]]
			} else {
				WrongLeader = true
			}
			wait = false
		} else {
			wait = true
		}
		sc.smMu.Unlock()
		if wait {
			time.Sleep(time.Duration(10) * time.Millisecond)
		} else {
			break
		}
	}
	if wait {
		WrongLeader = true
	}
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{
		OpType: QueryOp,
		Args:   *args,
	}
	reply.WrongLeader, reply.Err, reply.Config = sc.wait(args.ClerkId, args.Seq, command)
}

func (sc *ShardCtrler) serverApply() {
	for msg := range sc.applyCh {
		if sc.Killed() {
			return
		}
		sc.smMu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			switch op.OpType {
			case QueryOp:
				args := op.Args.(QueryArgs)
				if lastSeq, ok := sc.AppliedSeq[args.ClerkId]; ok && args.Seq <= lastSeq {
					//该命令已执行过
				} else {
					if args.Num != -1 {
						sc.ReplyConfigIndex[args.ClerkId] = min(args.Num, len(sc.configs)-1)
					} else {
						sc.ReplyConfigIndex[args.ClerkId] = len(sc.configs) - 1
					}
					sc.AppliedSeq[args.ClerkId] = args.Seq
				}
			case JoinOp:
				args := op.Args.(JoinArgs)
				if lastSeq, ok := sc.AppliedSeq[args.ClerkId]; ok && args.Seq <= lastSeq {
					//该命令已执行过
				} else {
					newConfig := sc.copyLastConfig()
					newConfig.Num++
					for GID, servers := range args.Servers {
						newConfig.Groups[GID] = make([]string, len(servers))
						copy(newConfig.Groups[GID], servers)
						sc.allocation[GID] = []int{}
					}
					sc.balanceShards(&newConfig)
					sc.configs = append(sc.configs, newConfig)
					sc.AppliedSeq[args.ClerkId] = args.Seq
					FPrintf("JoinOp", "args %v", args)
				}
			case LeaveOp:
				args := op.Args.(LeaveArgs)
				if lastSeq, ok := sc.AppliedSeq[args.ClerkId]; ok && args.Seq <= lastSeq {
					//该命令已执行过
				} else {
					newConfig := sc.copyLastConfig()
					newConfig.Num++
					for _, GID := range args.GIDs {
						for _, s := range sc.allocation[GID] {
							newConfig.Shards[s] = 0
						}
						delete(sc.allocation, GID)
						delete(newConfig.Groups, GID)
					}
					sc.balanceShards(&newConfig)
					sc.configs = append(sc.configs, newConfig)
					sc.AppliedSeq[args.ClerkId] = args.Seq
				}
			case MoveOp:
				args := op.Args.(MoveArgs)
				if lastSeq, ok := sc.AppliedSeq[args.ClerkId]; ok && args.Seq <= lastSeq {
					//该命令已执行过
				} else {
					newConfig := sc.copyLastConfig()
					newConfig.Num++
					oldOwner := newConfig.Shards[args.Shard]
					if oldOwner != 0 {
						i := 0
						for sc.allocation[oldOwner][i] != args.Shard {
							i++
						}
						sc.allocation[oldOwner] = append(sc.allocation[oldOwner][0:i], sc.allocation[oldOwner][i+1:]...)
					}
					sc.allocation[args.GID] = append(sc.allocation[args.GID], args.Shard)
					newConfig.Shards[args.Shard] = args.GID
					sc.balanceShards(&newConfig)
					sc.configs = append(sc.configs, newConfig)
					sc.AppliedSeq[args.ClerkId] = args.Seq
				}
			}
			sc.appliedIndex = msg.CommandIndex
		}
		sc.smMu.Unlock()
	}
}

func (sc *ShardCtrler) balanceShards(cfg *Config) {
	// fmt.Printf("before rebalance %v\n", sc.allocation)
	if len(cfg.Groups) == 0 {
		return
	}
	unallocated := []int{}
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] == 0 {
			unallocated = append(unallocated, i)
		}
	}
	avg := NShards / len(cfg.Groups)
	rich := []int{}
	poor := []int{}
	all := []int{}
	for g, allocated := range sc.allocation {
		if len(allocated) < avg {
			poor = append(poor, g)
		} else if len(allocated) > avg {
			rich = append(rich, g)
		}
		all = append(all, g)
	}
	sort.Ints(rich)
	sort.Ints(poor)
	sort.Ints(all)
	//fmt.Printf("unallocated: %v\n", unallocated)
	//fmt.Printf("poor: %v\n", poor)
	//fmt.Printf("rich: %v\n", rich)
	for _, g := range poor {
		for len(sc.allocation[g]) < avg {
			if len(unallocated) > 0 {
				sc.allocation[g] = append(sc.allocation[g], unallocated[0])
				unallocated = unallocated[1:]
			} else {
				giver := rich[0]
				sc.allocation[g] = append(sc.allocation[g], sc.allocation[giver][0])
				sc.allocation[giver] = sc.allocation[giver][1:]
				if len(sc.allocation[giver]) <= avg {
					rich = rich[1:]
				}
			}
		}
	}
	for _, g := range all {
		if len(unallocated) == 0 {
			break
		}
		sc.allocation[g] = append(sc.allocation[g], unallocated[0])
		unallocated = unallocated[1:]
	}
	for g, shards := range sc.allocation {
		for _, s := range shards {
			cfg.Shards[s] = g
		}
	}
	//fmt.Printf("after rebalance %v\n", sc.allocation)
}

func (sc *ShardCtrler) copyLastConfig() Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num,
		Groups: map[int][]string{},
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = lastConfig.Shards[i]
	}
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
	}
	return newConfig
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	sc.AppliedSeq = make(map[int64]int64)
	sc.ReplyConfigIndex = make(map[int64]int)
	sc.allocation = map[int][]int{}
	go sc.serverApply()
	return sc
}
