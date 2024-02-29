package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int64
	Index   int64
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	n             int
	leaderId      int
	lastHeartBeat int64
	applyCh       chan ApplyMsg
	lastApplied   int64
	commitIndex   int64

	// persistency
	currentTerm   int64
	voteFor       int
	log           []LogEntry
	snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int64

	// for leader
	nextIndex  []int64
	matchIndex []int64
	lastSend   []int64

	// for bebug
	lastLock int64
	holder   string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = rf.leaderId == rf.me
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.SnapshotTerm)
	e.Encode(rf.SnapshotIndex)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil {
		panic("error when decode rf.currentTerm")
	}
	if d.Decode(&rf.voteFor) != nil {
		panic("error when decode rf.voteFor")
	}
	if d.Decode(&rf.log) != nil {
		panic("error when decode rf.log")
	}
	d.Decode(&rf.SnapshotTerm)
	d.Decode(&rf.SnapshotIndex)
	rf.snapshot = rf.persister.ReadSnapshot()
	//FPrintf("readPersist", "read server state:\n%v", rf.Info())
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//FPrintf("Snapshot", "before Snapshot:\n%v", rf.Info())
	rf.lock("Snapshot")
	rf.snapshot = snapshot
	lastSnapLogPos := rf.findLogPosByIndex(int64(index)) // 必然存在lastSnapLog
	lastSnapLog := rf.log[lastSnapLogPos]
	rf.SnapshotTerm = lastSnapLog.Term
	rf.SnapshotIndex = lastSnapLog.Index
	rf.log = rf.log[lastSnapLogPos+1:]
	rf.persist()
	//FPrintf("Snapshot", "after Snapshot:\n%v", rf.Info())
	rf.mu.Unlock()

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogTerm  int64
	LastLogIndex int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	grant := false
	stateChange := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = -1
		rf.voteFor = -1
		stateChange = true
	}
	if args.Term == rf.currentTerm {
		grant = (rf.voteFor == -1 || rf.voteFor == args.CandidateId)

		// 检查candidate的log信息
		lastLogTerm, lastLogIndex := rf.lastLogTermAndIndex()
		switch {
		case args.LastLogTerm < lastLogTerm:
			grant = false
		case args.LastLogTerm == lastLogTerm:
			if args.LastLogIndex < lastLogIndex {
				grant = false
			}
		}
	}
	if grant {
		rf.lastHeartBeat = time.Now().UnixMilli()
		rf.voteFor = args.CandidateId
		FPrintf("RequestVote", "node %v vote args: %v\n", rf.me, args)
		//FPrintf("RequestVote", "node info:\n%v", rf.Info())
		stateChange = true
	} else {
		FPrintf("RequestVote", "server %v refuse vote args: %v\n", rf.me, args)
		//FPrintf("RequestVote", "node info:\n%v", rf.Info())
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = grant
	if stateChange {
		rf.persist()
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isLeader = rf.leaderId == rf.me
	//rf.mu.Unlock()
	if isLeader {
		//rf.mu.Lock()
		_, lastIndex := rf.lastLogTermAndIndex()
		entry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   lastIndex + 1,
		}
		rf.log = append(rf.log, entry)
		index = int(entry.Index)
		term = int(entry.Term)
		rf.persist()
		//rf.mu.Unlock()
		for i := 0; i < rf.n; i++ {
			if i == rf.me {
				continue
			}
			go func(workingTerm int64, peer int) {
				rf.sendAppendRequest(workingTerm, peer)
			}(rf.currentTerm, i)
		}
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//type CheckTermArgs struct {
//	Term int64
//}
//
//type CheckTermReply struct {
//	Term int64
//}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		isLeader := rf.leaderId == rf.me
		currentTerm := rf.currentTerm
		lastHeartBeat := rf.lastHeartBeat
		rf.mu.Unlock()
		now := time.Now().UnixMilli()
		if !isLeader && now-lastHeartBeat > 400 {
			go rf.runElection(currentTerm + 1)
			FPrintf("ticker", "server %v start a election for term %v now: %v lastHeart %v\n", rf.me, currentTerm+1, now, lastHeartBeat)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) runElection(electingTerm int64) {

	rf.mu.Lock()
	if rf.currentTerm >= electingTerm { // 选举时机已过
		rf.mu.Unlock()
		return
	}
	rf.leaderId = -1
	rf.currentTerm = electingTerm
	rf.voteFor = rf.me
	rf.lastHeartBeat = time.Now().UnixMilli()
	lastLogTerm, lastLogIndex := rf.lastLogTermAndIndex()
	rf.persist()
	rf.mu.Unlock()

	replyChan := make(chan *RequestVoteReply)
	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			go func(p int) {
				args := RequestVoteArgs{
					CandidateId:  rf.me,
					Term:         electingTerm,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				ok := rf.peers[p].Call("Raft.RequestVote", &args, &reply)
				if ok {
					replyChan <- &reply
				} else {
					replyChan <- nil
				}
			}(i)
		}
	}
	vote := 1
	neg := 0
	for i := 0; i < rf.n-1; i++ {
		r := <-replyChan
		if r == nil {
			neg++
			continue
		}
		if r.VoteGranted {
			vote++
		} else {
			neg++
			if rf.handleHigherTerm(r.Term) {
				return
			}
		}
		if neg > rf.n/2 || vote > rf.n/2 {
			break
		}
	}
	if vote > rf.n/2 {
		FPrintf("runElection", "server %v get %v votes for term %v\n", rf.me, vote, electingTerm)
		rf.takeOffice(electingTerm)
	}
}
func (rf *Raft) handleHigherTerm(term int64) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.leaderId = -1
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
		return true
	} else {
		return false
	}
}

type InstallSnapshotArgs struct {
	Term              int64
	LeaderId          int
	LastIncludedIndex int64
	LastIncludedTerm  int64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int64
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.mu.Unlock()
	changeState := false
	defer func(change *bool) {
		reply.Term = rf.currentTerm
		if *change {
			rf.persist()
		}
	}(&changeState)
	//FPrintf("InstallSnapshot", "before install snapshot:\n%v", rf.Info())
	if args.Term < rf.currentTerm {
		return
	}
	rf.lastHeartBeat = time.Now().UnixMilli()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		changeState = true
	}
	if rf.SnapshotIndex >= args.LastIncludedIndex { // 没有必要更新快照
		return
	} else {
		changeState = true
	}
	lastSnapLogPos := rf.findLogPosByIndex(args.LastIncludedIndex)
	rf.snapshot = args.Data
	rf.SnapshotIndex = args.LastIncludedIndex
	rf.SnapshotTerm = args.LastIncludedTerm
	// 清除不必要的log

	if lastSnapLogPos >= len(rf.log) {
		rf.log = []LogEntry{}
	} else {
		rf.log = rf.log[lastSnapLogPos+1:]
	}
	//FPrintf("InstallSnapshot", "install snapshot influence:\n%v", rf.Info())
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	LeaderCommit int64
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term     int64
	LeaderId int
	Success  bool
	XTerm    int64 //term in the conflicting entry (if any)
	XIndex   int64 //index of first entry with that term (if any)
	XLen     int64 //log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock()

	defer func() {
		reply.Term = rf.currentTerm
		reply.LeaderId = rf.leaderId
	}()

	changeState := false
	defer func(change *bool) {
		reply.Term = rf.currentTerm
		if *change {
			rf.persist()
		}
	}(&changeState)

	if args.Term < rf.currentTerm {
		// 过期的请求,不处理
		reply.Success = false
		return
	} else {
		rf.lastHeartBeat = time.Now().UnixMilli()
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.voteFor = args.LeaderId
		changeState = true
	}

	FPrintf("AppendEntries", "server %v hears from leader %v at %v\n", rf.me, args.LeaderId, rf.lastHeartBeat)

	//处理AppendEntries请求
	prevLogPos := rf.findLogPosByIndex(args.PrevLogIndex)
	oldCommit := rf.commitIndex
	switch {
	case prevLogPos < 0: // prevLog在snapshot中
		reply.Success = true
		lastMatchLogIndex := args.PrevLogIndex // followr和leader相同日志的最大index
		if prevLogPos+len(args.Entries) < 0 {  // args中的Entries都已经在snapshot中
			reply.Success = true
		} else {
			rf.mergeLog(args.Entries[-prevLogPos-1:], 0)
			changeState = true
			lastMatchLogIndex = args.Entries[len(args.Entries)-1].Index
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, lastMatchLogIndex)
		}
	case prevLogPos < len(rf.log):
		prevLog := rf.log[prevLogPos]
		if prevLog.Term != args.PrevLogTerm {
			reply.Success = false
			reply.XTerm = prevLog.Term
			reply.XIndex = rf.firstIndexOfTerm(reply.XTerm) // rf.log至少存在prevLog
		} else {
			lastMatchIndex := args.PrevLogIndex
			if len(args.Entries) > 0 {
				i := prevLogPos + 1
				j := 0
				for i < len(rf.log) && j < len(args.Entries) && rf.log[i].Term == args.Entries[j].Term {
					i++
					j++
				}
				if j != len(args.Entries) {
					rf.log = append(rf.log[:i], args.Entries[j:]...)
					rf.persist()
				}
				lastNewLog := args.Entries[len(args.Entries)-1]
				lastMatchIndex = lastNewLog.Index
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, lastMatchIndex)
			}
			reply.Success = true
		}
	default:
		reply.Success = false
		reply.XLen = int64(len(rf.log)) + rf.SnapshotIndex
	}

	// debug
	if reply.Success {
		FPrintf("AppendEntries", "server %v accept append %v\n", rf.me, args)
		if rf.commitIndex > oldCommit {
			FPrintf("AppendEntries", "server %v commit [%v...%v]\n", rf.me, oldCommit+1, rf.commitIndex)
		}
		//FPrintf("AppendEntries", "server info:\n %v", rf.Info())
	} else {
		FPrintf("AppendEntries", "server %v reject append %v\n", rf.me, args)
		//FPrintf("AppendEntries", "server info:\n %v", rf.Info())
	}
}

func (rf *Raft) mergeLog(entries []LogEntry, startPos int) {
	i := startPos
	j := 0
	for i < len(rf.log) && j < len(entries) && rf.log[i].Term == entries[j].Term {
		i++
		j++
	}
	if j != len(entries) {
		rf.log = append(rf.log[:i], entries[j:]...)
	}
}

func (rf *Raft) firstIndexOfTerm(term int64) int64 {
	low, high := 0, len(rf.log)-1
	for low <= high {
		mid := (high-low)/2 + low
		if rf.log[mid].Term >= term {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	if low >= len(rf.log) || rf.log[low].Term != term {
		return -1
	} else {
		return int64(low)
	}
}

func (rf *Raft) lastIndexOfTerm(term int64) int64 {
	low, high := 0, len(rf.log)-1
	for low <= high {
		mid := (high-low)/2 + low
		if rf.log[mid].Term > term {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	if high < 0 || rf.log[high].Term != term {
		return -1
	} else {
		return int64(high)
	}
}

// 调用前不能持有锁
func (rf *Raft) takeOffice(term int64) {
	rf.mu.Lock()
	_, lastLogIndex := rf.lastLogTermAndIndex()
	if term == rf.currentTerm {
		rf.leaderId = rf.me
		for i := 0; i < rf.n; i++ {
			if i != rf.me {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
		}
		// 启动心跳协程
		go rf.heartBeat(rf.currentTerm)
		FPrintf("takeOffice", "node %v become leader of term %v", rf.me, rf.currentTerm)
		//FPrintf("takeOffice", "leader Info:\n%v", rf.Info())
	}
	rf.mu.Unlock()
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		startTime := time.Now().UnixMilli()
		msgs := []ApplyMsg{}
		rf.mu.Lock()

		// 检查是否需要恢复快照
		if rf.SnapshotIndex > rf.lastApplied {
			msgs = append(msgs, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: int(rf.SnapshotIndex),
				SnapshotTerm:  int(rf.SnapshotTerm),
			})
			rf.lastApplied = rf.SnapshotIndex
			rf.commitIndex = max(rf.commitIndex, rf.SnapshotIndex)
		}

		// 检查是否需要应用日志
		if rf.commitIndex > rf.lastApplied {
			begin := rf.findLogPosByIndex(rf.lastApplied)
			end := rf.findLogPosByIndex(rf.commitIndex)
			for i := begin + 1; i <= end; i++ {
				msgs = append(msgs, ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: int(rf.log[i].Index)})
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
			FPrintf("apply", "server %v commit %v\n", rf.me, msg)
		}
		endTime := time.Now().UnixMilli()
		mms := max(0, 100-(endTime-startTime))
		time.Sleep(time.Duration(mms) * time.Microsecond)
	}

}

func (rf *Raft) findLogPosByIndex(index int64) int {
	return int(index - rf.SnapshotIndex - 1)
}

type appendRequest struct {
	target          int
	args            *AppendEntriesArgs
	reply           *AppendEntriesReply
	installSnapshot bool
	snapshotArgs    *InstallSnapshotArgs
	snapshotReply   *InstallSnapshotReply
}

// 尝试向peer发送AppendRequest,收到成功的回复返回或过期返回
func (rf *Raft) sendAppendRequest(workingTerm int64, peer int) {
	rf.lock("sendAppendRequest")
	request := rf.buildAppendRequest(peer)
	//lastSend := rf.lastSend[peer]
	rf.unlock()
	//if time.Now().UnixMilli()-lastSend < 10 { // 避免频繁向同一个peer发送
	//	return
	//}
	ok := false
	success := false
	for rf.leaderTerm() == workingTerm && (!ok || !success) {
		if ok { // success == false leader向follower添加日志失败,需要调整nextIndex installSnapshot不会导致success为false
			reply := request.reply
			rf.lock("sendAppendRequest")
			switch {
			case reply.Term > rf.currentTerm:
				rf.currentTerm = reply.Term
				rf.leaderId = reply.LeaderId
				rf.voteFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			case reply.Term < rf.currentTerm:
				// 由于workingTerm <= reply.Term, reply.Term < rf.currentTerm说明该节点已进入新term
				rf.mu.Unlock()
				return
			case reply.Term == rf.currentTerm:
				target := request.target
				if reply.XTerm > 0 {
					lastIndex := rf.lastIndexOfTerm(reply.XTerm)
					switch {
					case reply.XTerm > rf.SnapshotTerm: // 匹配的term应该在log中
						if lastIndex < 0 { // log中不存在该term
							rf.nextIndex[target] = reply.XIndex
						} else {
							rf.nextIndex[target] = lastIndex + 1
						}
					case reply.XTerm == rf.SnapshotTerm: // XTerm在snapshot中,还可能在log中
						if lastIndex < 0 { // log中不存在该term, snapshot的末尾是XTerm的最后一个log
							rf.nextIndex[target] = rf.SnapshotIndex + 1
						} else {
							rf.nextIndex[target] = lastIndex + 1
						}
					case reply.XTerm < rf.SnapshotTerm: // XTerm应该在snapshot中
						rf.nextIndex[target] = rf.SnapshotIndex
					}
				} else {
					rf.nextIndex[target] = reply.XLen
				}
				request = rf.buildAppendRequest(target)
				rf.lastSend[target] = time.Now().UnixMilli()
			}
			rf.unlock()
		}
		FPrintf("sendAppendRequest", "server %v try to send appendRequest to server %v args: %v \n", rf.me, request.target, request.args)
		sendBegin := time.Now().UnixMilli()
		if request.installSnapshot {
			ok = rf.peers[request.target].Call("Raft.InstallSnapshot", request.snapshotArgs, request.snapshotReply)
		} else {
			ok = rf.peers[request.target].Call("Raft.AppendEntries", request.args, request.reply)
			FPrintf("sendAppendRequest", "server %v has sent appendRequest to server %v using %v ms args: %v \n", rf.me, request.target, time.Now().UnixMilli()-sendBegin, request.args)
		}
		if ok {
			success = request.installSnapshot || request.reply.Success
		}
	}

	if ok && success {
		//处理success的请求
		if request.installSnapshot {
			args, reply := request.snapshotArgs, request.snapshotReply
			rf.lock("sendAppendRequest")
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[request.target] = max(rf.nextIndex[request.target], args.LastIncludedIndex+1)
				rf.matchIndex[request.target] = max(rf.matchIndex[request.target], args.LastIncludedIndex)
			}
			rf.unlock()
		} else {
			reply := request.reply
			args := request.args
			rf.lock("sendAppendRequest")
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.leaderId = reply.LeaderId
				rf.persist()
			} else if reply.Term < rf.currentTerm {
				// 过期的回复,直接忽略
			} else {
				// 成功向follower添加日志
				if len(args.Entries) > 0 {
					lastEntry := &args.Entries[len(args.Entries)-1]
					rf.nextIndex[request.target] = max(lastEntry.Index+1, rf.nextIndex[request.target])
					rf.matchIndex[request.target] = max(lastEntry.Index, rf.matchIndex[request.target])
				} else {
					rf.matchIndex[request.target] = max(args.PrevLogIndex, rf.matchIndex[request.target])
				}
				oldCommit := rf.commitIndex
				rf.updateCommitIndex()
				if rf.commitIndex > oldCommit {
					FPrintf("sendAppendRequest", "leader %v commit [%v...%v]\n", rf.me, oldCommit+1, rf.commitIndex)
					//FPrintf("sendAppendRequest", "leader info:\n%v", rf.Info())
				}
			}
			rf.unlock()
		}
	}

}

// 计算新的commitIndex
func (rf *Raft) updateCommitIndex() {
	low := rf.commitIndex
	_, high := rf.lastLogTermAndIndex()
	for high >= low {
		mid := (high-low)/2 + low
		cnt := 1
		for i := 0; i < rf.n; i++ {
			if i != rf.me && rf.matchIndex[i] >= mid {
				cnt++
			}
		}
		if cnt > rf.n/2 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	rf.commitIndex = high
}

// 当前节点为leader且未被kill时返回当前term,否则返回-1
func (rf *Raft) leaderTerm() int64 {
	term := int64(-1)
	rf.mu.Lock()
	if rf.dead == 0 && rf.leaderId == rf.me {
		term = rf.currentTerm
	}
	rf.mu.Unlock()
	return term
}

func (rf *Raft) heartBeat(workingTerm int64) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.currentTerm != workingTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < rf.n; i++ {
			if i == rf.me {
				continue
			}
			go rf.sendAppendRequest(workingTerm, i)
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

// 构建发送给peer的appendRequest, 调用前需持有锁
func (rf *Raft) buildAppendRequest(peer int) (request appendRequest) {
	request.target = peer
	nextLogPos := rf.findLogPosByIndex(rf.nextIndex[peer])
	prevLogTerm, preLogIndex := rf.SnapshotTerm, rf.SnapshotIndex
	switch {
	case nextLogPos < 0: // 要发送的Log已被压缩
		request.installSnapshot = true
		request.snapshotArgs = &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.leaderId,
			LastIncludedIndex: rf.SnapshotIndex,
			LastIncludedTerm:  rf.SnapshotTerm,
			Data:              rf.snapshot,
		}
		request.snapshotReply = &InstallSnapshotReply{}
	case nextLogPos > 0: // 要发送的Log是快照之外的第一个
		prevLog := rf.log[nextLogPos-1]
		prevLogTerm, preLogIndex = prevLog.Term, prevLog.Index
		fallthrough
	case nextLogPos == 0:
		lastLogTerm, _ := rf.lastLogTermAndIndex()
		var entries []LogEntry
		if lastLogTerm == rf.currentTerm {
			entries = rf.log[nextLogPos:]
		}
		request.args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.leaderId,
			PrevLogIndex: preLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}
		request.reply = &AppendEntriesReply{}
	}
	return request
}

func (rf *Raft) Info() string {
	//rf.mu.Lock()
	//defer rf.mu.Lock()
	lastLogTerm, lastLogIndex := rf.lastLogTermAndIndex()
	ret := fmt.Sprintf("node: %v\n", rf.me) +
		fmt.Sprintf("term: %v\n", rf.currentTerm) +
		fmt.Sprintf("leaderId: %v\n", rf.leaderId) +
		fmt.Sprintf("lastLogIndex: %v\n", lastLogIndex) +
		fmt.Sprintf("LastLogTerm: %v\n", lastLogTerm) +
		fmt.Sprintf("commitIndex: %v\n", rf.commitIndex) +
		fmt.Sprintf("snapshotTerm: %v\n", rf.SnapshotTerm) +
		fmt.Sprintf("snapshotIndex: %v\n", rf.SnapshotIndex) +
		fmt.Sprintf("logs: %v\n", rf.log)
	if rf.leaderId == rf.me {
		ret = ret + fmt.Sprintf("nextIndex: %v\n", rf.nextIndex) +
			fmt.Sprintf("matchIndex: %v\n", rf.matchIndex)
	}
	return ret
}

func (rf *Raft) lastLogTermAndIndex() (int64, int64) {
	if len(rf.log) > 0 {
		lastLog := rf.log[len(rf.log)-1]
		return lastLog.Term, lastLog.Index
	} else {
		return rf.SnapshotTerm, rf.SnapshotIndex
	}
}

func (rf *Raft) lock(holder string) {
	begin := time.Now().UnixMilli()
	rf.mu.Lock()
	finish := time.Now().UnixMilli()
	if finish-begin > 200 {
		FPrintf("lock", "wait too long (%v ms) waiting for %v lock", finish-begin, holder)
	}
	rf.lastLock = time.Now().UnixMilli()
	rf.holder = holder
}

func (rf *Raft) unlock() {
	now := time.Now().UnixMilli()
	if now-rf.lastLock > 200 {
		FPrintf("unlock", "%v hold lock of server %v too long %vms\n", rf.holder, rf.me, now-rf.lastLock)
	}
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.n = len(peers)
	rf.leaderId = -1
	rf.applyCh = applyCh

	rf.log = []LogEntry{}
	rf.nextIndex = make([]int64, rf.n)
	rf.matchIndex = make([]int64, rf.n)
	rf.lastSend = make([]int64, rf.n)
	rf.lastHeartBeat = time.Now().UnixMilli()
	rf.snapshot = []byte{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动协程将提交的命令应用到状态机
	go rf.apply()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
