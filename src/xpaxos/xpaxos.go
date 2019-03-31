package xpaxos

import (
	"labrpc"
	"sync"
)

type LogEntry struct {
	Operation interface{}
}

// A Go object implementing a single XPaxos peer
type XPaxos struct {
	mu               Sync.Mutex
	replicas         []*labrpc.ClientEnd // Contains all replicas including this one
	synchronousGroup []*labrpc.ClientEnd
	id               int
	leaderId         int
	prepareSeqNum    int
	executeSeqNum    int
	prepareLog       []LogEntry
	commitLog        []LogEntry
}

func Make(replicas []*labrpc.ClientEnd, id int, persister *Persister) *XPaxos {
	xp := &XPaxos{}

	xp.mu.Lock()
	xp.replicas = replicas
	xp.persister = persister
	xp.id = id
	xp.leaderId = 1
	xp.prepareSeqNum = 0
	xp.executeSeqNum = 0
	xp.prepareLog = make([]LogEntry, 0)
	xp.commitLog = make([]LogEntry, 0)
	xp.mu.Unlock()

	return xp
}

func main() {
	print("hello world")
}
