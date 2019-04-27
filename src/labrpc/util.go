package labrpc

import "log"

// Debugging (0 = None, 1 = Info, 2 = Debug)
const DEBUG = 1

func dPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 1 {
		log.Printf(format, a...)
	}
	return
}

func iPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 0 {
		log.Printf(format, a...)
	}
	return
}
