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
	// By default reply.IsLeader = false and reply.Success = false
	xp.mu.Lock()
	msgDigest := digest(request)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if xp.id == xp.getLeader() { // If XPaxos server is the leader
		reply.IsLeader = true

		if len(xp.prepareLog) > 0 && request.Timestamp <= xp.prepareLog[len(xp.prepareLog)-1].Msg0.ClientTimestamp {
			xp.mu.Unlock()
			reply.Success = true
			return
		}

		xp.prepareSeqNum++

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

		numReplies := len(xp.synchronousGroup) - 1
		replyCh := make(chan bool, numReplies)

		for server, _ := range xp.synchronousGroup {
			if server != xp.id {
				go xp.issuePrepare(server, prepareEntry, replyCh)
			}
		}

		xp.persist()
		xp.mu.Unlock()

		timer := time.NewTimer(TIMEOUT * time.Millisecond).C

		for i := 0; i < numReplies; i++ {
			select {
			case <-timer:
				iPrintf("Timeout: XPaxos.Replicate: XPaxos server (%d)\n", xp.id)
				return
			case <-replyCh:
			}
		}

		xp.mu.Lock()
		xp.executeSeqNum++
		xp.persist()
		reply.Success = true
	}
	xp.mu.Unlock()
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
		xp.mu.Lock()
		verification := xp.verify(server, reply.MsgDigest, reply.Signature)
		xp.mu.Unlock()

		if bytes.Compare(prepareEntry.Msg0.MsgDigest[:], reply.MsgDigest[:]) == 0 && verification == true {
			if reply.Success == true {
				replyCh <- reply.Success
			} else if reply.Suspicious == true {
				return
			}
		} else { // Verification of crypto signature in reply fails
			go xp.issueSuspect()
		}
	} else { // RPC times out after time frame delta (see labrpc)
		go xp.issueSuspect()
	}
}

func (xp *XPaxos) Prepare(prepareEntry PrepareLogEntry, reply *Reply) {
	// By default reply.Success = false and reply.Suspicious = false
	xp.mu.Lock()
	msgDigest := digest(prepareEntry.Request)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if prepareEntry.Msg0.PrepareSeqNum == xp.prepareSeqNum+1 && bytes.Compare(prepareEntry.Msg0.MsgDigest[:],
		msgDigest[:]) == 0 && xp.verify(prepareEntry.Msg0.SenderId, msgDigest, prepareEntry.Msg0.Signature) == true {
		if len(xp.prepareLog) > 0 && prepareEntry.Request.Timestamp <= xp.prepareLog[len(xp.prepareLog)-1].Msg0.ClientTimestamp {
			reply.Success = true
			xp.mu.Unlock()
			return
		}

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

		numReplies := len(xp.synchronousGroup) - 1
		replyCh := make(chan bool, numReplies)

		for server, _ := range xp.synchronousGroup {
			if server != xp.id {
				go xp.issueCommit(server, msg, replyCh)
			}
		}

		xp.persist()
		xp.mu.Unlock()

		timer := time.NewTimer(TIMEOUT * time.Millisecond).C

		for i := 0; i < numReplies; i++ {
			select {
			case <-timer:
				iPrintf("Timeout: XPaxos.Prepare: XPaxos server (%d)\n", xp.id)
				return
			case <-replyCh:
			}
		}

		timer = time.NewTimer(TIMEOUT * time.Millisecond).C

		// Busy wait until XPaxos server receives commit messages from entire synchronous group
		xp.mu.Lock()
		for len(xp.commitLog[xp.executeSeqNum].Msg0) != len(xp.synchronousGroup) {
			xp.mu.Unlock()
			select {
			case <-timer:
				iPrintf("Timeout: XPaxos.Prepare: XPaxos server (%d)\n", xp.id)
				return
			default:
				time.Sleep(10 * time.Millisecond)
			}
			xp.mu.Lock()
		}

		xp.executeSeqNum++
		xp.persist()
		reply.Success = true
	} else { // Verification of crypto signature in prepareEntry fails
		reply.Suspicious = true
		go xp.issueSuspect()
	}
	xp.mu.Unlock()
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
		xp.mu.Lock()
		verification := xp.verify(server, reply.MsgDigest, reply.Signature)
		xp.mu.Unlock()

		if bytes.Compare(msg.MsgDigest[:], reply.MsgDigest[:]) == 0 && verification == true {
			if reply.Success == true {
				replyCh <- reply.Success
			} else if reply.Suspicious == true {
				return
			} else {
				go xp.issueCommit(server, msg, replyCh) // Retransmit if commit RPC fails - DO NOT CHANGE
			}
		} else { // Verification of crypto signature in reply fails
			go xp.issueSuspect()
		}
	} else { // RPC times out after time frame delta (see labrpc)
		go xp.issueSuspect()
	}
}

func (xp *XPaxos) Commit(msg Message, reply *Reply) {
	// By default reply.Success == false
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
			xp.persist()
			reply.Success = true
		}
	} else { // Verification of crypto signature in msg fails
		reply.Suspicious = true
		go xp.issueSuspect()
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
	xp.suspectSet = make(map[[32]byte]SuspectMessage, 0)
	xp.vcSet = make(map[[32]byte]ViewChangeMessage, 0)
	xp.netFlag = false
	xp.netTimer = nil
	xp.vcFlag = false
	xp.vcTimer = nil
	xp.receivedVCFinal = make(map[int]map[[32]byte]ViewChangeMessage, 0)

	xp.generateSynchronousGroup(int64(xp.view))
	xp.readPersist(persister.ReadXPaxosState())
	xp.mu.Unlock()

	return xp
}

func (xp *XPaxos) Kill() {}
