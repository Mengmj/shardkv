package kvsrv

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	server *labrpc.ClientEnd

	// You will have to modify this struct.
	clerkId int64
	seq     atomic.Int64
	mu      sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.seq = atomic.Int64{}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	getArgs := GetArgs{Key: key, ClerkId: ck.clerkId, Seq: ck.seq.Add(1)}
	getReply := GetReply{}
	ok := false
	ck.mu.Lock()
	defer ck.mu.Unlock()
	for !ok {
		ok = ck.server.Call("KVServer.Get", &getArgs, &getReply)
	}
	ck.Ack(getArgs.Seq)
	return getReply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	putAppendArgs := PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.clerkId, Seq: ck.seq.Add(1)}
	putAppendReply := PutAppendReply{}
	ok := false
	ck.mu.Lock()
	defer ck.mu.Unlock()
	for !ok {
		ok = ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply)
		//DPrintf("try with putAppend %v\n", putAppendArgs)
	}
	go ck.Ack(putAppendArgs.Seq)
	return putAppendReply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Ack(seq int64) {
	ackArgs := AckArgs{ClerkId: ck.clerkId, Seq: seq}
	ackReply := Empty{}
	ck.server.Call("KVServer.Ack", &ackArgs, &ackReply)
}
