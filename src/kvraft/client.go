package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.Mutex
	leader int
	id     int64
	seq    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

// fetch the current result for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:     key,
		ClerkId: ck.id,
		Seq:     ck.seq,
	}
	ck.seq++
	var reply GetReply
	success := false
	for !success {
		reply = GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				success = true
			case ErrWrongLeader:
				ck.leader = (ck.leader + 1) % len(ck.servers)
			default:
				panic(fmt.Sprintf("unexpected GetReply.Err: %v", reply.Err))
			}
		} else {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
	return reply.Value
}

// shared by PutOp and AppendOp.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.id,
		Seq:     ck.seq,
	}
	ck.seq++
	var reply PutAppendReply
	success := false
	for !success {
		reply = PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				success = true
			case ErrWrongLeader:
				ck.leader = (ck.leader + 1) % len(ck.servers)
			default:
				panic(fmt.Sprintf("unexpected PutAppendReply.Err: %v", reply.Err))
			}
		} else {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
