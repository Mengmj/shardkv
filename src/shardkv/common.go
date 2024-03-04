package shardkv

import (
	"fmt"
	"log"
	"os"
	"sync"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	Seq     int64
	Shard   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	Seq     int64
	Shard   int
}

type GetReply struct {
	Err   Err
	Value string
}

var flags = map[string]interface{}{
	"test":           nil,
	"PutAppend":      nil,
	"wait":           nil,
	"queryConfig":    nil,
	"consume":        nil,
	"updateConfig":   nil,
	"moveShard":      nil,
	"execute":        nil,
	"makeSnapshot":   nil,
	"resumeSnapshot": nil,
}

const PringF = true

func FPrint(flag string, format string, a ...interface{}) {
	if _, ok := flags[flag]; ok && PringF {
		log.Printf(flag+":::"+format, a...)
	}
}

type logger struct {
	owner  string
	output *os.File
	mu     sync.Mutex
}

func makeLogger(owner string) *logger {
	l := logger{
		owner: owner,
	}
	f, err := os.Create(fmt.Sprintf("%v.log", owner))
	if err != nil {
		panic("fail create log file")
	}
	l.output = f
	return &l
}
func (l *logger) log(format string, a ...interface{}) {
	l.mu.Lock()
	msg := fmt.Sprintf(format, a...)
	_, err := fmt.Fprintf(l.output, msg)
	if err != nil {
		panic("log err")
	}
	l.mu.Unlock()
}
