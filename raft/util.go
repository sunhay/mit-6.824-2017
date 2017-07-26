package raft

import "log"

// Debugging
const Debug = 1

func LogInfo(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[INFO] " + format, a...)
	}
	return
}

func LogDebug(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		log.Printf("[DEBUG] " + format, a...)
	}
	return
}


