package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func CPrintf(format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	if Debug {
		log.Printf("\033[1;31;40m%s\033[0m", s)
	}
}
