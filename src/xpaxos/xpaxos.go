package xpaxos

import (
	"bytes"
	"crypto/rsa"
	"labrpc"
	"time"
)

//
// ---------------------------- REPLICATE/REPLY RPC ---------------------------
//
func (xp *XPaxos) Replicate(request ClientRequest, reply *Reply) {
	if xp.id == xp.getLeader() { // If XPaxos server is the leader
		if len(xp.prepareLog) > 0 && request.Timestamp <= xp.prepareLog[len(xp.prepareLog)-1].Msg0.ClientTimestamp {
			reply.IsLeader = true
			reply.Success = false
			return
		}

		xp.mu.Lock()
		xp.prepareSeqNum++
		msgDigest := digest(request)
		signature := xp.sign(msgDigest)

		msg := Message{
			MsgType:         PREPARE,
			MsgDigest:       msgDigest,
			Signature:       signature,
			PrepareSeqNum:   xp.prepareSeqNum,
			View:            xp.view,
			ClientTimestamp: request.Timestamp,
			SenderId:        xp.id}

		prepareEntry := xp.appendToPrepareLog(request, msg)

		msgMap := make(map[int]Message, 0)
		msgMap[xp.id] = msg // Leader's prepare message
		xp.appendToCommitLog(request, msgMap)

		replyCh := make(chan bool, len(xp.synchronousGroup)-1)
		xp.mu.Unlock()

		for server, _ := range xp.synchronousGroup {
			if server != xp.id {
				go xp.issuePrepare(server, prepareEntry, replyCh)
			}
		}

		for i := 0; i < len(xp.synchronousGroup)-1; i++ {
			<-replyCh
		}

		xp.mu.Lock()
		xp.executeSeqNum++
		reply.IsLeader = true
		reply.Success = true
		xp.mu.Unlock()
	} else {
		reply.IsLeader = false
		reply.Success = false
	}
}

//
// -------------------------------- PREPARE RPC -------------------------------
//
func (xp *XPaxos) sendPrepare(server int, prepareEntry PrepareLogEntry, reply *Reply) bool {
	dPrintf("Prepare: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.Prepare", prepareEntry, reply, xp.id)
}

func (xp *XPaxos) issuePrepare(server int, prepareEntry PrepareLogEntry, replyCh chan bool) {
	reply := &Reply{}

	if ok := xp.sendPrepare(server, prepareEntry, reply); ok {
		if bytes.Compare(prepareEntry.Msg0.MsgDigest[:], reply.MsgDigest[:]) == 0 && xp.verify(server, reply.MsgDigest,
			reply.Signature) == true {
			if reply.Success == true {
				replyCh <- reply.Success
			} else {
				go xp.issuePrepare(server, prepareEntry, replyCh)
			}
		}
	}
}

func (xp *XPaxos) Prepare(prepareEntry PrepareLogEntry, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	msgDigest := digest(prepareEntry.Request)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if prepareEntry.Msg0.PrepareSeqNum == xp.prepareSeqNum+1 && bytes.Compare(prepareEntry.Msg0.MsgDigest[:],
		msgDigest[:]) == 0 && xp.verify(prepareEntry.Msg0.SenderId, msgDigest, prepareEntry.Msg0.Signature) == true {
		xp.prepareSeqNum++
		xp.prepareLog = append(xp.prepareLog, prepareEntry)

		msg := Message{
			MsgType:         COMMIT,
			MsgDigest:       msgDigest,
			Signature:       signature,
			PrepareSeqNum:   xp.prepareSeqNum,
			View:            xp.view,
			ClientTimestamp: prepareEntry.Request.Timestamp,
			SenderId:        xp.id}

		if xp.executeSeqNum >= len(xp.commitLog) {
			msgMap := make(map[int]Message, 0)
			msgMap[xp.getLeader()] = prepareEntry.Msg0 // Leader's prepare message
			msgMap[xp.id] = msg                        // Follower's commit message
			xp.appendToCommitLog(prepareEntry.Request, msgMap)
		}

		replyCh := make(chan bool, len(xp.synchronousGroup)-1)
		xp.mu.Unlock()

		for server, _ := range xp.synchronousGroup {
			if server != xp.id {
				go xp.issueCommit(server, msg, replyCh)
			}
		}

		for i := 0; i < len(xp.synchronousGroup)-1; i++ {
			<-replyCh
		}

		xp.mu.Lock()

		for len(xp.commitLog[xp.executeSeqNum].Msg0) != len(xp.synchronousGroup) {
			xp.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			xp.mu.Lock()
		}

		xp.executeSeqNum++
		reply.Success = true
	} else {
		reply.Success = false
	}
}

//
// --------------------------------- COMMIT RPC --------------------------------
//
func (xp *XPaxos) sendCommit(server int, msg Message, reply *Reply) bool {
	dPrintf("Commit: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.Commit", msg, reply, xp.id)
}

func (xp *XPaxos) issueCommit(server int, msg Message, replyCh chan bool) {
	reply := &Reply{}

	if ok := xp.sendCommit(server, msg, reply); ok {
		if bytes.Compare(msg.MsgDigest[:], reply.MsgDigest[:]) == 0 && xp.verify(server, reply.MsgDigest,
			reply.Signature) == true {
			if reply.Success == true {
				replyCh <- true
			} else {
				go xp.issueCommit(server, msg, replyCh)
			}
		}
	}
}

func (xp *XPaxos) Commit(msg Message, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	msgDigest := msg.MsgDigest
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if xp.verify(msg.SenderId, msgDigest, msg.Signature) == true {
		if xp.executeSeqNum < len(xp.commitLog) {
			senderId := msg.SenderId
			xp.commitLog[xp.executeSeqNum].Msg0[senderId] = msg
			reply.Success = true
		} else {
			reply.Success = false
		}
	}
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func Make(replicas []*labrpc.ClientEnd, id int, persister *Persister, privateKey *rsa.PrivateKey,
	publicKeys map[int]*rsa.PublicKey) *XPaxos {
	xp := &XPaxos{}

	xp.mu.Lock()
	xp.persister = persister
	xp.replicas = replicas
	xp.synchronousGroup = make(map[int]bool, 0)
	xp.id = id
	xp.view = 1
	xp.prepareSeqNum = 0
	xp.executeSeqNum = 0
	xp.prepareLog = make([]PrepareLogEntry, 0)
	xp.commitLog = make([]CommitLogEntry, 0)
	xp.privateKey = privateKey
	xp.publicKeys = publicKeys

	xp.generateSynchronousGroup(int64(xp.getLeader()))
	xp.readPersist(persister.ReadXPaxosState())
	xp.mu.Unlock()

	return xp
}

func (xp *XPaxos) Kill() {}
