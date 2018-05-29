package raft

import (
    "log"
    "os"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Printf(format, a...)
		l := log.New(os.Stdout, "", log.LstdFlags | log.Lmicroseconds)
		l.Printf(format, a...)
	}
	return
}
