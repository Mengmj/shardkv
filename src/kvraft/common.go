package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	PutOp          = "PutOp"
	AppendOp       = "AppendOp"
	GetOp          = "GetOp"
)

type Err string

// PutOp or AppendOp
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "PutOp" or "AppendOp"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	Seq     int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	Seq     int64
}

type GetReply struct {
	Err   Err
	Value string
}
