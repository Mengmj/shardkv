package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op      string
	ClerkId int64
	Seq     int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	Seq     int64
}

type GetReply struct {
	Value string
}

type AckArgs struct {
	ClerkId int64
	Seq     int64
}

type Empty struct {
}
