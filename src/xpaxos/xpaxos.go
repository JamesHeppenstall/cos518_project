package xpaxos

import (
	"fmt"
	"labrpc"
	"sync"
	"time"
)

type ApplyMsg struct {
	Operation interface{}
}

type LogEntry struct {
	Operation interface{}
}

// A Go object implementing a single XPaxos peer
type XPaxos struct {
	mu               sync.Mutex
	persister        *Persister
	replicas         []*labrpc.ClientEnd // Contains all replicas including this one
	synchronousGroup []*labrpc.ClientEnd
	Id               int
	view             int
	leaderId         int
	prepareSeqNum    int
	executeSeqNum    int
	prepareLog       []LogEntry
	commitLog        []LogEntry
}

func (xp *XPaxos) GetState() (int, bool) {
	var isLeader bool

	xp.mu.Lock()
	defer xp.mu.Unlock()

	view := xp.view

	if xp.Id == xp.leaderId {
		isLeader = true
	} else {
		isLeader = false
	}

	return view, isLeader
}

type Args struct {
	str string
}

type Reply struct {
	str string
}

func (xp *XPaxos) PrintString(args Args, reply *Reply) {
	fmt.Printf("Hello World\n")
}

func (xp *XPaxos) sendRPC(server int, args Args, reply *Reply) bool {
	ok := xp.replicas[server].Call("XPaxos.PrintString", args, reply)
	return ok
}

func Make(replicas []*labrpc.ClientEnd, id int, persister *Persister, applyCh chan ApplyMsg) *XPaxos {
	xp := &XPaxos{}

	xp.mu.Lock()
	xp.replicas = replicas
	xp.persister = persister
	xp.Id = id
	xp.leaderId = 1
	xp.prepareSeqNum = 0
	xp.executeSeqNum = 0
	xp.prepareLog = make([]LogEntry, 0)
	xp.commitLog = make([]LogEntry, 0)
	xp.mu.Unlock()

	go func() {
		fmt.Printf("Id: %d", xp.Id)
		args := Args{}
		args.str = "Hello"

		reply := Reply{}
		if ok := xp.sendRPC(0, args, &reply); ok {
			time.Sleep(1 * time.Second)
		}
	}()

	return xp
}

func (xp *XPaxos) Kill() {
}
