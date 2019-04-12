package xpaxos

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
    "encoding/gob"
	"labrpc"
	"sync"
)

const (
	REPLICATE = iota
	PREPARE   = iota
	COMMIT    = iota
	REPLY     = iota
)

type Message struct {
	MsgType         int
	MsgDigest       [16]byte
	PrepareSeqNum   int
	View            int
	ClientTimestamp int
}

type PrepareLogEntry struct {
	Request ClientRequest
	Msg0    Message
}

type CommitLogEntry struct {
	Request ClientRequest
	Msg0    Message
	Msg1    Message
}

type XPaxos struct {
	mu               sync.Mutex
	persister        *Persister
	replicas         []*labrpc.ClientEnd
	synchronousGroup []*labrpc.ClientEnd
	id               int
	view             int
	prepareSeqNum    int
	executeSeqNum    int
	prepareLog       []PrepareLogEntry
	commitLog        []CommitLogEntry
}

//
// ------------------------------ HELPER FUNCTIONS -----------------------------
//
func (xp *XPaxos) GetState() (int, bool) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	isLeader := false
	view := xp.view

	if xp.id == view {
		isLeader = true
	}

	return view, isLeader
}

func digest(msg interface{}) [16]byte {
	jsonBytes, _ := json.Marshal(msg)
	return md5.Sum(jsonBytes)
}

func (xp *XPaxos) persist() {
    buf := new(bytes.Buffer)
    enc := gob.NewEncoder(buf)
    enc.Encode(0)
    data := buf.Bytes()
    xp.persister.SaveXPaxosState(data)
}

func (xp *XPaxos) readPersist(data []byte) {
    buf := bytes.NewBuffer(data)
    dec := gob.NewDecoder(buf)
    dec.Decode(0)
}



//
// ------------------------------ REPLICATE/REPLY RPC -----------------------------
//

type ReplicateReply struct {
	Msg1 Message
}

func (xp *XPaxos) Replicate(args ClientRequest, reply *ReplicateReply) {
	xp.IssueCommit(2, args)

	msg := Message {
			MsgType:         REPLY,
			PrepareSeqNum:   xp.prepareSeqNum,
			View:            xp.view,
			ClientTimestamp: args.Timestamp}

	reply.Msg1 = msg
}

//
// --------------------------------- COMMIT RPC --------------------------------
//
type CommitReply struct {
	Msg1 Message
}

func (xp *XPaxos) Commit(args PrepareLogEntry, reply *CommitReply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	digest := digest(args.Request)

	if args.Msg0.PrepareSeqNum == xp.prepareSeqNum+1 && bytes.Compare(args.Msg0.MsgDigest[:], digest[:]) == 0 {
		xp.prepareSeqNum++
		xp.executeSeqNum++

		msg := Message{
			MsgType:         COMMIT,
			MsgDigest:       digest,
			PrepareSeqNum:   xp.prepareSeqNum,
			View:            xp.view,
			ClientTimestamp: args.Request.Timestamp}

		entry := CommitLogEntry{
			Request: args.Request,
			Msg0:    args.Msg0,
			Msg1:    msg}

		xp.commitLog = append(xp.commitLog, entry);

		reply.Msg1 = msg
	}
}

func (xp *XPaxos) IssueCommit(receiverId int, request ClientRequest) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	xp.prepareSeqNum++
	digest1 := digest(request)

	msg := Message{
		MsgType:         COMMIT,
		MsgDigest:       digest1,
		PrepareSeqNum:   xp.prepareSeqNum,
		View:            xp.view,
		ClientTimestamp: -1}

	prepareEntry := PrepareLogEntry{
		Request: request,
		Msg0:    msg}

	xp.prepareLog = append(xp.prepareLog, prepareEntry)

	reply := CommitReply{}

	if ok := xp.sendCommit(receiverId, prepareEntry, &reply); ok {
		digest2 := digest(xp.prepareLog[xp.prepareSeqNum-1].Request)

		if bytes.Compare(reply.Msg1.MsgDigest[:], digest2[:]) == 0 {
			commitEntry := CommitLogEntry{
				Request: request,
				Msg0:    msg,
				Msg1:    reply.Msg1}

			xp.commitLog = append(xp.commitLog, commitEntry)

			xp.executeSeqNum++
			// xp.checkCommitLog()
		}
	}
}

func (xp *XPaxos) sendCommit(receiverId int, args PrepareLogEntry, reply *CommitReply) bool {
    return xp.replicas[receiverId].Call("XPaxos.Commit", args, reply)
}

func (xp *XPaxos) checkCommitLog() {
	for {
		xp.mu.Lock()

		if len(xp.commitLog) > xp.executeSeqNum+1 {
			xp.executeSeqNum++
			return
		}

		xp.mu.Unlock()
	}
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func Make(replicas []*labrpc.ClientEnd, id int, persister *Persister) *XPaxos {
	xp := &XPaxos{}

	xp.mu.Lock()
	xp.persister = persister
	xp.replicas = replicas
	xp.synchronousGroup = replicas
	xp.id = id
	xp.view = 1
	xp.prepareSeqNum = 0
	xp.executeSeqNum = 0
	xp.prepareLog = make([]PrepareLogEntry, 0)
	xp.commitLog = make([]CommitLogEntry, 0)

    xp.readPersist(persister.ReadXPaxosState())
    xp.mu.Unlock()

	return xp
}

func (xp *XPaxos) Kill() {
}
