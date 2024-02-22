package kvsrv

import (
	"hash/fnv"
	"log"

	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	memoryCapacity int
	memoryMap      map[int64]*memory
	memoryQueue    []*memory
	bucketNum      int
	bucketLocks    []sync.RWMutex
	buckets        []map[string]*string
}

type memory struct {
	cid   int64
	mu    sync.Mutex
	ack   int64
	onWay int64
	value *string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cid := args.ClerkId
	seq := args.Seq

	mem := kv.getMemory(cid)

	mem.mu.Lock()
	defer mem.mu.Unlock()
	if seq <= mem.ack {
		return
	}
	if seq > mem.onWay {
		mem.ack = mem.onWay
		mem.onWay = seq
		mem.value = kv.get(args.Key)
	}
	reply.Value = *mem.value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cid := args.ClerkId
	seq := args.Seq
	mem := kv.getMemory(cid)

	mem.mu.Lock()
	defer mem.mu.Unlock()
	if seq <= mem.ack {
		return
	}
	if seq > mem.onWay {
		mem.ack = mem.onWay
		mem.onWay = seq
		mem.value = kv.put(args.Key, args.Value)
	}
	reply.Value = *mem.value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cid := args.ClerkId
	seq := args.Seq
	mem := kv.getMemory(cid)

	mem.mu.Lock()
	defer mem.mu.Unlock()
	if seq <= mem.ack {
		return
	}
	if seq > mem.onWay {
		mem.ack = mem.onWay
		mem.onWay = seq
		mem.value = kv.append(args.Key, args.Value)
	}
	reply.Value = *mem.value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.memoryCapacity = 1000
	kv.bucketNum = 100
	kv.bucketLocks = make([]sync.RWMutex, kv.bucketNum)
	kv.buckets = make([]map[string]*string, kv.bucketNum)
	for i := 0; i < kv.bucketNum; i++ {
		kv.buckets[i] = map[string]*string{}
	}
	kv.memoryMap = make(map[int64]*memory)
	return kv
}

func (kv *KVServer) get(key string) *string {
	b := ihash(key) % kv.bucketNum
	bucketLock := &kv.bucketLocks[b]
	bucketLock.RLock()
	value, ok := kv.buckets[b][key]
	if !ok {
		t := ""
		value = &t
	}
	bucketLock.RUnlock()
	return value
}

func (kv *KVServer) append(key string, value string) *string {
	b := ihash(key) % kv.bucketNum
	bucketLock := &kv.bucketLocks[b]
	bucketLock.Lock()
	old, ok := kv.buckets[b][key]
	if !ok {
		t := ""
		old = &t
	}
	newValue := *old + value
	kv.buckets[b][key] = &newValue
	bucketLock.Unlock()
	return old
}

func (kv *KVServer) put(key string, value string) *string {
	b := ihash(key) % kv.bucketNum
	bucketLock := &kv.bucketLocks[b]
	bucketLock.Lock()
	kv.buckets[b][key] = &value
	bucketLock.Unlock()
	return &value
}

func (kv *KVServer) Ack(args *AckArgs, repley *Empty) {
	cid := args.ClerkId
	seq := args.Seq
	kv.mu.Lock()
	mem, ok := kv.memoryMap[cid]
	kv.mu.Unlock()
	if ok {
		mem.mu.Lock()
		if seq == mem.onWay {
			mem.ack = mem.onWay
			mem.value = nil
		}
		mem.mu.Unlock()
	}
}

func (kv *KVServer) getMemory(cid int64) *memory {
	kv.mu.Lock()
	mem, ok := kv.memoryMap[cid]
	if !ok {
		memStruct := memory{cid: cid}
		mem = &memStruct
		if len(kv.memoryQueue) > kv.memoryCapacity {
			discard := kv.memoryQueue[0]
			delete(kv.memoryMap, discard.cid)
			kv.memoryQueue = kv.memoryQueue[1:]
		}
		kv.memoryQueue = append(kv.memoryQueue, mem)
		kv.memoryMap[cid] = mem
	}
	kv.mu.Unlock()
	return mem
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
