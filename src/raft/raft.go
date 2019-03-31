package raft

//
// This is an outline of the API that raft must expose to the service (or tester). See comments
// below for each of these functions for more details.
//
// rf = Make(...)
//     Create a new Raft server
// rf.Start(command interface{}) (index, term, isLeader)
//     Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//     Ask a Raft peer for its current term, and whether it thinks it is leader
// ApplyMsg
//     Each time a new entry is committed to the log, each Raft peer should send an ApplyMsg to the
//     service (or tester) in the same server
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	FOLLOWER  = iota
	CANDIDATE = iota
	LEADER    = iota
)

//
// As each Raft peer becomes aware that successive log entries are committed, the peer should send
// an ApplyMsg to the service (or tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Ignore for assignment3
	Snapshot    []byte // Ignore for assignment3
}

//
// A Go object implementing a single log entry.
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int // Index into peers[]
	state         int
	currentTerm   int
	votedFor      int
	log           []LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	numberVotes   map[int]bool
	appendEntries chan bool
	applyCh       chan ApplyMsg
}

//
// Return currentTerm and whether this server believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var isLeader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm

	if rf.state == LEADER {
		isLeader = true
	} else {
		isLeader = false
	}

	return term, isLeader
}

//
// Update currentTerm and revert Raft server to follower state.
//
func (rf *Raft) revertToFollower(newTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist()
}

//
// Append entry with given command to Raft server's log
//
func (rf *Raft) appendToLog(command interface{}) {
	entry := LogEntry{}
	entry.Term = rf.currentTerm
	entry.Command = command
	rf.log = append(rf.log, entry)
}

//
// Apply committed commands to state machine using applyCh
//
func (rf *Raft) applyToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.persist()
		applyMsg := ApplyMsg{}
		applyMsg.Index = rf.lastApplied
		applyMsg.Command = rf.log[rf.lastApplied].Command
		rf.applyCh <- applyMsg
	}
}

//
// Save Raft's persistent state to stable storage, where it can later be retrieved after a crash and
// restart. See paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// Restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(&rf.currentTerm)
	dec.Decode(&rf.votedFor)
	dec.Decode(&rf.log)
}

//
// RequstVote RPC for leader election.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.revertToFollower(args.Term)
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	reply.Term = args.Term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogTerm >= lastLogTerm {
		if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
			reply.VoteGranted = false
		} else {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	} else {
		reply.VoteGranted = false
	}
}

//
// AppendEntries RPC for log replication and heartbeat.
//
type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	select {
	case rf.appendEntries <- true:
	default:
	}

	reply.ConflictIndex = len(rf.log)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.revertToFollower(args.Term)
	}

	rf.state = FOLLOWER
	reply.Term = args.Term

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.Success = false
		reply.ConflictIndex = 0

		for rf.log[reply.ConflictIndex].Term != conflictTerm {
			reply.ConflictIndex++
		}

		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	nextIndex := len(rf.log) - 1
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < nextIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = nextIndex
		}

		go rf.applyToStateMachine()
	}
}

//
// Server is the index of the target server in rf.peers[], expects RPC arguments in args, and fills
// in *reply with RPC reply, so caller should pass &reply. The types of the args and reply passed
// to Call() must be  the same as the types of the arguments declared in the handler function
// (including whether they are pointers).
//
// Returns true if labrpc says the RPC was delivered.
//
// If you're having trouble getting RPC to work, check that you've capitalized all field names in
// structs passed over RPC, and that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// The service using Raft (e.g. a k/v server) wants to start agreement on the next command to be
// appended to Raft's log. If this server isn't the leader, return false, otherwise start the
// agreement and return immediately. There is no guarantee that this command will ever be committed
// to the Raft log, since the leader may fail or lose an election.
//
// The first return value is the index that the command will appear at if it's ever committed, the
// second return value is the current term, and the third return value is true if this server
// believes it is the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	if rf.state != LEADER {
		return index, term, isLeader
	}

	rf.appendToLog(command)
	rf.persist()
	index = len(rf.log) - 1
	term = rf.currentTerm
	isLeader = true

	return index, term, isLeader
}

//
// The tester calls Kill() when a Raft instance won't be needed again. you are not required to do
// anything in Kill(), but it might be convenient to (for example) turn off debug output from this
// instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired
}

//
// The service or tester wants to create a Raft server. The ports of all the Raft servers (including
// this one) are in peers[]. This server's port is peers[me]. All the servers' peers[] arrays have
// the same order. persister is a place for this server to save its persistent state, and also
// initially holds the most recent saved state, if any. applyCh is a channel on which the tester or
// service expects Raft to send ApplyMsg messages. Make() must return quickly, so it should start
// goroutines for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.numberVotes = make(map[int]bool)
	rf.appendEntries = make(chan bool)
	rf.applyCh = applyCh

	// Initialize first entry as null placeholder in log
	rf.appendToLog(nil)

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	go func() {
		for {
			switch rf.state {
			case LEADER:
				rf.runLeader()
			case FOLLOWER:
				rf.runFollower()
			case CANDIDATE:
				rf.runCandidate()
			}
		}
	}()

	return rf
}

//
// The Raft server runs this function as a follower.
//
func (rf *Raft) runFollower() {
	timer := time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond).C

	select {
	case <-timer:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.state = CANDIDATE
	case <-rf.appendEntries:
	}
}

//
// The Raft server runs these functions as a candidate.
//
func (rf *Raft) runCandidate() {
	timer := time.NewTimer(time.Duration(50+rand.Intn(50)) * time.Millisecond).C // Raft election timeout
	args := RequestVoteArgs{}

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.numberVotes = make(map[int]bool)
	rf.numberVotes[rf.me] = true
	rf.persist()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server != rf.me {
			go rf.issueRequestVote(server, args)
		}
	}

	select {
	case <-timer:
	case <-rf.appendEntries:
	}
}

func (rf *Raft) issueRequestVote(server int, args RequestVoteArgs) {
	reply := RequestVoteReply{}

	if ok := rf.sendRequestVote(server, args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.revertToFollower(reply.Term)
			return
		}

		if args.Term != rf.currentTerm {
			return
		}

		if reply.VoteGranted == true {
			rf.numberVotes[server] = true
		}

		if rf.state != LEADER && len(rf.numberVotes) > len(rf.peers)/2 {
			rf.state = LEADER

			for server, _ := range rf.peers {
				rf.nextIndex[server] = len(rf.log)
				rf.matchIndex[server] = 0
			}
		}
	}
}

//
// The Raft server runs these functions as a leader.
//
func (rf *Raft) runLeader() {
	rf.leaderCommitIndex()
	args := AppendEntriesArgs{}

	for server, _ := range rf.peers {
		if server != rf.me {
			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex

			if rf.nextIndex[server] < 1 {
				args.PrevLogIndex = 0
			} else if rf.nextIndex[server] > len(rf.log) {
				args.PrevLogIndex = len(rf.log) - 1
			} else {
				args.PrevLogIndex = rf.nextIndex[server] - 1
			}

			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1:]
			rf.mu.Unlock()

			go rf.issueAppendEntries(server, args)
		}
	}

	time.Sleep(time.Duration(35) * time.Millisecond) // Heartbeat timeout
}

func (rf *Raft) issueAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}

	if ok := rf.sendAppendEntries(server, args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.revertToFollower(reply.Term)
			return
		}

		if args.Term != rf.currentTerm {
			return
		}

		if reply.Success == true {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else if reply.Success == false {
			if reply.ConflictIndex == 0 || reply.ConflictIndex > rf.nextIndex[server] {
				rf.nextIndex[server]--
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}
}

func (rf *Raft) leaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[rf.me] = len(rf.log) - 1
	numberMatches := 0
	N := len(rf.log) - 1

	for N > rf.commitIndex {
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= N {
				numberMatches++
			}
		}

		if numberMatches > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			go rf.applyToStateMachine()
			return
		}

		numberMatches = 0
		N--
	}
}
