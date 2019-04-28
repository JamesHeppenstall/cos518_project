package xpaxos

// RPC handlers for the XPaxos common case (replicate, prepare, commit, reply)
// XPaxos operates under a system model called Cross Fault-Tolerance (XFT) that lies
// between Crash Fault-Tolerance (CFT) and Byzantine Fault-Tolerance (BFT)
//
// XFT Assumptions:
// (1) Clients and replicas can suffer Byzantine faults
// (2) All replicas share reliable bi-directional communication channels
// (3) An *eventually synchronous* network model (i.e. there always exists a majority
//     of replicas - a synchronous group - that can send RPCs within some time frame
//     delta)
//
// We simulate a network in the eponymous package - in particular, this allows gives us
// fine-grained control over the time frame delta (defined in network/common.go - line 9)
//
// xp := Make(replicas, id, persister, privateKey, publicKeys) - Creates an XPaxos server
// => Option to perform cleanup with xp.Kill()

import (
	"bytes"
	"crypto/rsa"
	"network"
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

		msg := Message{ // Leader's prepare message
			MsgType:         PREPARE,
			MsgDigest:       msgDigest,
			Signature:       signature,
			PrepareSeqNum:   xp.prepareSeqNum,
			View:            xp.view,
			ClientTimestamp: request.Timestamp,
			SenderId:        xp.id}

		prepareEntry := xp.appendToPrepareLog(request, msg)

		msgMap := make(map[int]Message, 0)
		xp.appendToCommitLog(request, msg, msgMap)

		numReplies := len(xp.synchronousGroup) - 1
		replyCh := make(chan bool, numReplies)

		for server, _ := range xp.synchronousGroup {
			if server != xp.id {
				go xp.issuePrepare(server, prepareEntry, replyCh)
			}
		}

		xp.persist()
		xp.mu.Unlock()

		timer := time.NewTimer(3 * network.DELTA * time.Millisecond).C

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
	xp.mu.Lock()
	oldView := xp.view
	xp.mu.Unlock()

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
			go xp.issueSuspect(oldView)
		}
	} else { // RPC times out after time frame delta (see network)
		go xp.issueSuspect(oldView)
	}
}

func (xp *XPaxos) Prepare(prepareEntry PrepareLogEntry, reply *Reply) {
	// By default reply.Success = false and reply.Suspicious = false
	xp.mu.Lock()
	msgDigest := digest(prepareEntry.Request)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if xp.view != prepareEntry.Msg0.View {
		xp.mu.Unlock()
		return
	}

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
			msgMap[xp.id] = msg                                                   // Follower's commit message
			xp.appendToCommitLog(prepareEntry.Request, prepareEntry.Msg0, msgMap) // Leader's prepare message is prepareEntry.Msg0
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

		timer := time.NewTimer(2 * network.DELTA * time.Millisecond).C

		for i := 0; i < numReplies; i++ {
			select {
			case <-timer:
				iPrintf("Timeout: XPaxos.Prepare: XPaxos server (%d)\n", xp.id)
				return
			case <-replyCh:
			}
		}

		timer = time.NewTimer(2 * network.DELTA * time.Millisecond).C

		// Busy wait until XPaxos server receives commit messages from entire synchronous group
		xp.mu.Lock()
		for len(xp.commitLog[xp.executeSeqNum].Msg1) != len(xp.synchronousGroup)-1 {
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
		go xp.issueSuspect(xp.view)
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
	xp.mu.Lock()
	oldView := xp.view
	xp.mu.Unlock()

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
			go xp.issueSuspect(oldView)
		}
	} else { // RPC times out after time frame delta (see network)
		go xp.issueSuspect(oldView)
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

	if xp.view != msg.View {
		reply.Suspicious = true
		return
	}

	if xp.verify(msg.SenderId, msgDigest, msg.Signature) == true {
		if xp.executeSeqNum < len(xp.commitLog) {
			senderId := msg.SenderId
			xp.commitLog[xp.executeSeqNum].Msg1[senderId] = msg
			xp.persist()
			reply.Success = true
		}
	} else { // Verification of crypto signature in msg fails
		reply.Suspicious = true
		go xp.issueSuspect(xp.view)
	}
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func Make(replicas []*network.ClientEnd, id int, persister *Persister, privateKey *rsa.PrivateKey,
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
