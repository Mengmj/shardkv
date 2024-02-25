package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

var flags = map[string]interface{}{
	//"readPersist": nil,
	"takeOffice": nil,
	//"apply": nil,
	"RequestVote":       nil,
	"runElection":       nil,
	"sendAppendEntries": nil,
	"ticker":            nil,
	"AppendEntries":     nil,
	"unlock":            nil,
	"lock":              nil,
	"sendAppendRequest": nil,
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func CPrintf(format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	if Debug {
		log.Printf("\033[1;31;47m%s\033[0m\n", s)
	}
}

func FPrintf(flag string, format string, a ...interface{}) {
	if _, ok := flags[flag]; ok && Debug {
		log.Printf(flag+":::"+format, a...)
	}
}
