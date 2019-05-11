package pbft

import (
	"crypto/rsa"
	"network"
)

//
// ---------------------------- REPLICATE/REPLY RPC ---------------------------
//
func (pbft *Pbft) Replicate(request ClientRequest, reply *Reply) {
	// By default reply.IsLeader = false and reply.Success = false
	msgDigest := digest(request)
	signature := pbft.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if pbft.id == pbft.getLeader() { // If PBFT server is the leader
		reply.IsLeader = true
		pbft.mu.Lock()
		pbft.prepareSeqNum = request.Timestamp

		msg := Message{ // Leader's prepare message
			MsgType:         PREPREPARE,
			MsgDigest:       msgDigest,
			Signature:       signature,
			PrepareSeqNum:   pbft.prepareSeqNum,
			View:            pbft.view,
			ClientTimestamp: request.Timestamp,
			SenderId:        pbft.id}

		prePrepareEntry := pbft.appendToPrepareLog(request, msg)
		pbft.mu.Unlock()
		for server, _ := range pbft.synchronousGroup {
			if server != pbft.id {
				go pbft.issuePrePrepare(server, prePrepareEntry)
			}
		}
		reply.Success = true
	}
}

//
// -------------------------------- PRE-PREPARE RPC -------------------------------
//
func (pbft *Pbft) sendPrePrepare(server int, prepareEntry PrepareLogEntry, reply *Reply) bool {
	dPrintf("PrePrepare: from Pbft server (%d) to Pbft server (%d)\n", pbft.id, server)
	return pbft.replicas[server].Call("Pbft.PrePrepare", prepareEntry, reply, pbft.id)
}

func (pbft *Pbft) issuePrePrepare(server int, prepareEntry PrepareLogEntry) {
	reply := &Reply{}
	if ok := pbft.sendPrePrepare(server, prepareEntry, reply); ok {

	}
}

func (pbft *Pbft) PrePrepare(prepareEntry PrepareLogEntry, reply *Reply) {
	// By default reply.Success = false and reply.Suspicious = false
	verification := pbft.verify(prepareEntry.Msg0.SenderId, prepareEntry.Msg0.MsgDigest, prepareEntry.Msg0.Signature)
	if verification == true && pbft.view == prepareEntry.Msg0.View {
		pbft.mu.Lock()
		if ok := pbft.addToPrepareLog(prepareEntry); !ok {
			prepareEntry.Hop = pbft.id
			pbft.mu.Unlock()
			for server, _ := range pbft.synchronousGroup {
				if server != pbft.id {
					go pbft.issuePrepare(server, prepareEntry)
				}
			}
			return
		}
		pbft.mu.Unlock()
	}
}

//
// -------------------------------- PREPARE RPC -------------------------------
//
func (pbft *Pbft) sendPrepare(server int, prepareEntry PrepareLogEntry, reply *Reply) bool {
	dPrintf("Prepare: from Pbft server (%d) to Pbft server (%d)\n", pbft.id, server)
	return pbft.replicas[server].Call("Pbft.Prepare", prepareEntry, reply, pbft.id)
}

func (pbft *Pbft) issuePrepare(server int, prepareEntry PrepareLogEntry) {
	reply := &Reply{}

	if ok := pbft.sendPrepare(server, prepareEntry, reply); ok {

	}
}

func (pbft *Pbft) Prepare(prepareEntry PrepareLogEntry, reply *Reply) {
	// By default reply.Success = false and reply.Suspicious = false
	verification := pbft.verify(prepareEntry.Msg0.SenderId, prepareEntry.Msg0.MsgDigest, prepareEntry.Msg0.Signature)

	if verification == true && pbft.view == prepareEntry.Msg0.View {
		pbft.mu.Lock()
		if ok := pbft.addToPrepareLog(prepareEntry); ok {
			if len(pbft.prepareLog[prepareEntry.Msg0.PrepareSeqNum].Msg1) >= 2*(len(pbft.replicas)-2)/3 {
				msgDigest := digest(prepareEntry.Request)
				signature := pbft.sign(msgDigest)
				pbft.prepareSeqNum = prepareEntry.Msg0.PrepareSeqNum

				msg := Message{
					MsgType:         COMMIT,
					MsgDigest:       msgDigest,
					Signature:       signature,
					PrepareSeqNum:   pbft.prepareSeqNum,
					View:            pbft.view,
					ClientTimestamp: prepareEntry.Request.Timestamp,
					SenderId:        pbft.id}

				cmsg := CommitMessage{
					msg, prepareEntry.Request}

				pbft.mu.Unlock()

				for server, _ := range pbft.synchronousGroup {
					go pbft.issueCommit(server, cmsg)
				}
				return
			}
		}
		pbft.mu.Unlock()
	}
}

//
// --------------------------------- COMMIT RPC --------------------------------
//
func (pbft *Pbft) sendCommit(server int, msg CommitMessage, reply *Reply) bool {
	dPrintf("Commit: from Pbft server (%d) to Pbft server (%d) for SeqNum %d\n", pbft.id, server, msg.Msg.ClientTimestamp)
	return pbft.replicas[server].Call("Pbft.Commit", msg, reply, pbft.id)
}

func (pbft *Pbft) issueCommit(server int, msg CommitMessage) {
	reply := &Reply{}

	if ok := pbft.sendCommit(server, msg, reply); ok {

	}
}

func (pbft *Pbft) Commit(msg CommitMessage, reply *Reply) {
	// By default reply.Success == false
	if pbft.view != msg.Msg.View {
		return
	}

	if pbft.verify(msg.Msg.SenderId, msg.Msg.MsgDigest, msg.Msg.Signature) == true {
		pbft.mu.Lock()
		if ok := pbft.addToCommitLog(msg); ok {
			if len(pbft.commitLog[msg.Msg.PrepareSeqNum].Msg1) >= 2*(len(pbft.replicas)-2)/3 && pbft.executeSeqNum < msg.Msg.PrepareSeqNum {
				dPrintf("Server %d SeqNum %d Commits %d ", pbft.id, msg.Msg.PrepareSeqNum, len(pbft.commitLog[msg.Msg.PrepareSeqNum].Msg1))
				pbft.executeSeqNum = msg.Msg.PrepareSeqNum
				pbft.mu.Unlock()
				go pbft.issueReply(msg)
				return
			}
		}
		pbft.mu.Unlock()
	}
}

//
// --------------------------------- REPLY RPC --------------------------------
//
func (pbft *Pbft) sendReply(creply ClientReply, reply *Reply) bool {
	iPrintf("Reply: from Pbft server (%d) to Pbft server (%d) for SeqNum %d\n", pbft.id, CLIENT, creply.Timestamp)
	return pbft.replicas[CLIENT].Call("Client.Reply", creply, reply, pbft.id)
}

func (pbft *Pbft) issueReply(msg CommitMessage) {
	reply := &Reply{}
	creply := ClientReply{pbft.id, msg.Request.Timestamp}

	if ok := pbft.sendReply(creply, reply); ok {

	}
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func Make(replicas []*network.ClientEnd, id int, privateKey *rsa.PrivateKey,
	publicKeys map[int]*rsa.PublicKey) *Pbft {
	pbft := &Pbft{}

	pbft.mu.Lock()
	pbft.replicas = replicas
	pbft.synchronousGroup = make(map[int]bool, 0)
	pbft.id = id
	pbft.view = 1
	pbft.prepareSeqNum = 0
	pbft.executeSeqNum = 0
	pbft.prepareLog = make([]PrepareLogEntry, 0)
	pbft.commitLog = make([]CommitLogEntry, 0)
	pbft.privateKey = privateKey
	pbft.publicKeys = publicKeys

	pbft.generateSynchronousGroup(int64(pbft.view))
	pbft.mu.Unlock()

	return pbft
}

func (pbft *Pbft) Kill() {}
