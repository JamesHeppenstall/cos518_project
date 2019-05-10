package xpaxos

import (
	"bytes"
	//"math/rand"
	"network"
	"time"
)

//
// -------------------------------- SUSPECT RPC -------------------------------
//
func (xp *XPaxos) sendSuspect(server int, msg SuspectMessage, reply *Reply) bool {
	//if xp.byzantine == true {
	//	for i := len(msg.Signature) - 1; i > 0; i-- {
	//		j := rand.Intn(i + 1)
	//		msg.Signature[i], msg.Signature[j] = msg.Signature[j], msg.Signature[i]
	//	}
	//}

	dPrintf("Suspect: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.Suspect", msg, reply, xp.id)
}

func (xp *XPaxos) issueSuspectHelper(server int, msg SuspectMessage) {
	reply := &Reply{}

	if ok := xp.sendSuspect(server, msg, reply); ok {
		xp.mu.Lock()
		if xp.view != msg.View {
			xp.mu.Unlock()
			return
		}

		verification := xp.verify(server, reply.MsgDigest, reply.Signature)

		if bytes.Compare(msg.MsgDigest[:], reply.MsgDigest[:]) != 0 || verification == false {
			go xp.issueSuspect(xp.view)
		}
		xp.mu.Unlock()
	} else {
		go xp.issueSuspect(msg.View)
	}
}

func (xp *XPaxos) issueSuspect(view int) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != view {
		return
	}

	msgDigest := digest(xp.view)
	signature := xp.sign(msgDigest)

	msg := SuspectMessage{
		MsgType:   SUSPECT,
		MsgDigest: msgDigest,
		Signature: signature,
		View:      xp.view,
		SenderId:  xp.id}

	for server, _ := range xp.replicas {
		if server != CLIENT {
			go xp.issueSuspectHelper(server, msg)
		}
	}
}

func (xp *XPaxos) forwardSuspect(msg SuspectMessage) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != msg.View+1 {
		return
	}

	for server, _ := range xp.replicas {
		if server != CLIENT {
			go xp.issueSuspectHelper(server, msg)
		}
	}
}

func (xp *XPaxos) Suspect(msg SuspectMessage, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	msgDigest := digest(msg.View)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	_, ok := xp.suspectSet[digest(msg)]

	if xp.view <= msg.View && ok == false {
		if bytes.Compare(msg.MsgDigest[:], msgDigest[:]) == 0 && xp.verify(msg.SenderId, msgDigest, msg.Signature) == true {
			xp.suspectSet[digest(msg)] = msg

			xp.view = msg.View + 1
			go xp.forwardSuspect(msg)

			xp.generateSynchronousGroup(int64(xp.view))
			xp.vcSet = make(map[[32]byte]ViewChangeMessage, 0)
			xp.receivedVCFinal = make(map[int]map[[32]byte]ViewChangeMessage, 0)
			xp.vcInProgress = true

			go xp.issueViewChange(xp.view)

			if len(xp.synchronousGroup) > 0 {
				xp.netFlag = false
				xp.netTimer = time.NewTimer(3 * network.DELTA * time.Millisecond).C
			}
		} else {
			go xp.issueSuspect(xp.view)
		}
	}
}

//
// ------------------------------ VIEW-CHANGE RPC -----------------------------
//
func (xp *XPaxos) sendViewChange(server int, msg ViewChangeMessage, reply *Reply) bool {
	//if xp.byzantine == true {
	//	for i := len(msg.Signature) - 1; i > 0; i-- {
	//		j := rand.Intn(i + 1)
	//		msg.Signature[i], msg.Signature[j] = msg.Signature[j], msg.Signature[i]
	//	}
	//}

	dPrintf("ViewChange: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.ViewChange", msg, reply, xp.id)
}

func (xp *XPaxos) issueViewChange(view int) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != view {
		return
	}

	msgDigest := digest(xp.view)
	signature := xp.sign(msgDigest)

	msg := ViewChangeMessage{
		MsgType:   VIEWCHANGE,
		MsgDigest: msgDigest,
		Signature: signature,
		View:      xp.view,
		SenderId:  xp.id,
		CommitLog: xp.commitLog}

	for server, _ := range xp.synchronousGroup {
		go func(xp *XPaxos, server int, msg ViewChangeMessage) {
			reply := &Reply{}

			if ok := xp.sendViewChange(server, msg, reply); ok {
				xp.mu.Lock()
				if xp.view != msg.View {
					xp.mu.Unlock()
					return
				}

				verification := xp.verify(server, reply.MsgDigest, reply.Signature)

				if bytes.Compare(msg.MsgDigest[:], reply.MsgDigest[:]) != 0 || verification == false {
					go xp.issueSuspect(xp.view)
				}
				xp.mu.Unlock()
			} else {
				go xp.issueSuspect(msg.View)
			}
		}(xp, server, msg)
	}
}

func (xp *XPaxos) ViewChange(msg ViewChangeMessage, reply *Reply) {
	xp.mu.Lock()
	msgDigest := digest(msg.View)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if xp.view == msg.View {
		if bytes.Compare(msg.MsgDigest[:], msgDigest[:]) == 0 && xp.verify(msg.SenderId, msgDigest, msg.Signature) == true {
			xp.vcSet[digest(msg)] = msg

			if len(xp.vcSet) == len(xp.replicas)-1 {
				xp.setVCTimer()
				go xp.issueVCFinal(xp.view)
				xp.mu.Unlock()
				return
			}
			xp.mu.Unlock()

			<-xp.netTimer

			xp.mu.Lock()
			if xp.view != msg.View {
				xp.mu.Unlock()
				return
			}

			if xp.netFlag == false && len(xp.vcSet) >= (len(xp.replicas)+1)/2 {
				xp.setVCTimer()
				go xp.issueVCFinal(xp.view)
			} else if xp.netFlag == false {
				xp.vcFlag = true
				go xp.issueSuspect(xp.view)
			}
		} else {
			go xp.issueSuspect(xp.view)
		}
	}
	xp.mu.Unlock()
}

//
// ------------------------------- VC-FINAL RPC -------------------------------
//
func (xp *XPaxos) sendVCFinal(server int, msg VCFinalMessage, reply *Reply) bool {
	//if xp.byzantine == true {
	//	for i := len(msg.Signature) - 1; i > 0; i-- {
	//		j := rand.Intn(i + 1)
	//		msg.Signature[i], msg.Signature[j] = msg.Signature[j], msg.Signature[i]
	//	}
	//}

	dPrintf("VCFinal: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.VCFinal", msg, reply, xp.id)
}

func (xp *XPaxos) issueVCFinal(view int) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != view {
		return
	}

	vcSetCopy := make(map[[32]byte]ViewChangeMessage)

	for msgDigest, msg := range xp.vcSet {
		vcSetCopy[msgDigest] = msg
	}

	msgDigest := digest(xp.view)
	signature := xp.sign(msgDigest)

	msg := VCFinalMessage{
		MsgType:   VCFINAL,
		MsgDigest: msgDigest,
		Signature: signature,
		View:      xp.view,
		SenderId:  xp.id,
		VCSet:     vcSetCopy}

	for server, _ := range xp.synchronousGroup {
		go func(xp *XPaxos, server int, msg VCFinalMessage) {
			reply := &Reply{}

			if ok := xp.sendVCFinal(server, msg, reply); ok {
				xp.mu.Lock()
				if xp.view != msg.View {
					xp.mu.Unlock()
					return
				}

				verification := xp.verify(server, reply.MsgDigest, reply.Signature)

				if bytes.Compare(msg.MsgDigest[:], reply.MsgDigest[:]) != 0 || verification == false {
					go xp.issueSuspect(xp.view)
				}
				xp.mu.Unlock()
			} else {
				go xp.issueSuspect(msg.View)
			}
		}(xp, server, msg)
	}
}

func (xp *XPaxos) VCFinal(msg VCFinalMessage, reply *Reply) {
	xp.mu.Lock()
	if xp.view != msg.View {
		xp.mu.Unlock()
		return
	}

	msgDigest := digest(msg.View)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	if bytes.Compare(msg.MsgDigest[:], msgDigest[:]) == 0 && xp.verify(msg.SenderId, msgDigest, msg.Signature) == true {
		for senderId, vcSet := range xp.receivedVCFinal { // This could/*should* be made more efficient
			for _, msg := range vcSet {
				if xp.view != msg.View {
					delete(xp.receivedVCFinal, senderId)
				}
			}
		}

		if xp.synchronousGroup[msg.SenderId] == true {
			xp.receivedVCFinal[msg.SenderId] = msg.VCSet

			if len(xp.receivedVCFinal) >= len(xp.synchronousGroup) {
				for _, msg := range msg.VCSet {
					xp.vcSet[digest(msg)] = msg
				}

				for _, msg := range xp.vcSet {
					for seqNum, _ := range msg.CommitLog {
						if len(xp.commitLog) <= seqNum {
							xp.commitLog = append(xp.commitLog, msg.CommitLog[seqNum])
						} else {
							if xp.commitLog[seqNum].View < msg.CommitLog[seqNum].View {
								xp.commitLog[seqNum] = msg.CommitLog[seqNum]
							}
						}
					}
				}

				if xp.id == xp.getLeader() {
					var request ClientRequest
					var msg0 Message
					var newMsg0 Message
					var msgDigest [32]byte
					var signature []byte

					for seqNum, _ := range xp.commitLog {
						request = xp.commitLog[seqNum].Request
						msg0 = xp.commitLog[seqNum].Msg0
						msgDigest = digest(request)
						signature = xp.sign(msgDigest)

						newMsg0 = Message{
							MsgType:         PREPARE,
							MsgDigest:       msgDigest,
							Signature:       signature,
							PrepareSeqNum:   seqNum + 1,
							View:            xp.view,
							ClientTimestamp: msg0.ClientTimestamp,
							SenderId:        msg0.SenderId}

						if seqNum < len(xp.prepareLog) {
							xp.updatePrepareLog(seqNum, request, newMsg0)
						} else {
							xp.appendToPrepareLog(request, newMsg0)
						}
					}

					msgDigest = digest(xp.view)
					signature = xp.sign(msgDigest)

					msg := NewViewMessage{
						MsgType:    NEWVIEW,
						MsgDigest:  msgDigest,
						Signature:  signature,
						View:       xp.view,
						PrepareLog: xp.prepareLog,
						SenderId:   xp.id}

					numReplies := len(xp.synchronousGroup) - 1
					replyCh := make(chan bool, numReplies)

					for server, _ := range xp.synchronousGroup {
						if server != xp.id {
							go xp.issueNewView(server, msg, replyCh)
						}
					}
					xp.mu.Unlock()

					timer := time.NewTimer(3 * network.DELTA * time.Millisecond).C

					for i := 0; i < numReplies; i++ {
						select {
						case <-timer:
							dPrintf("Timeout: XPaxos.VCFinal: XPaxos server (%d)\n", xp.id)
							return
						case <-replyCh:
						}
					}

					xp.mu.Lock()
					if xp.view != msg.View {
						xp.mu.Unlock()
						return
					}

					go xp.issueNewView(xp.id, msg, replyCh)
				}
			}
		}
	} else {
		go xp.issueSuspect(xp.view)
	}
	xp.mu.Unlock()
}

//
// -------------------------------- NEW-VIEW RPC ------------------------------
//
func (xp *XPaxos) sendNewView(server int, msg NewViewMessage, reply *Reply) bool {
	//if xp.byzantine == true {
	//	for i := len(msg.Signature) - 1; i > 0; i-- {
	//		j := rand.Intn(i + 1)
	//		msg.Signature[i], msg.Signature[j] = msg.Signature[j], msg.Signature[i]
	//	}
	//}

	dPrintf("NewView: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.NewView", msg, reply, xp.id)
}

func (xp *XPaxos) issueNewView(server int, msg NewViewMessage, replyCh chan bool) {
	reply := &Reply{}

	if ok := xp.sendNewView(server, msg, reply); ok {
		xp.mu.Lock()
		if xp.view != msg.View {
			xp.mu.Unlock()
			return
		}

		verification := xp.verify(server, reply.MsgDigest, reply.Signature)

		if bytes.Compare(msg.MsgDigest[:], reply.MsgDigest[:]) == 0 && verification == true {
			if reply.Success == true {
				replyCh <- reply.Success
			}
		} else {
			go xp.issueSuspect(xp.view)
		}
		xp.mu.Unlock()
	} else {
		go xp.issueSuspect(msg.View)
	}
}

func (xp *XPaxos) NewView(msg NewViewMessage, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != msg.View {
		return
	}

	msgDigest := digest(msg.View)
	signature := xp.sign(msgDigest)
	reply.MsgDigest = msgDigest
	reply.Signature = signature

	xp.vcFlag = true

	if bytes.Compare(msg.MsgDigest[:], msgDigest[:]) == 0 && xp.verify(msg.SenderId, msgDigest, msg.Signature) == true {
		if xp.compareLogs(msg.PrepareLog, xp.commitLog) {
			xp.prepareLog = msg.PrepareLog
			xp.prepareSeqNum = len(xp.prepareLog)
			xp.executeSeqNum = len(xp.commitLog)

			xp.suspectSet = make(map[[32]byte]SuspectMessage, 0)
			xp.vcSet = make(map[[32]byte]ViewChangeMessage, 0)
			xp.receivedVCFinal = make(map[int]map[[32]byte]ViewChangeMessage, 0)
			xp.vcInProgress = false

			if xp.id == xp.getLeader() {
				go xp.issueConfirmVC()
			}

			reply.Success = true
		} else {
			go xp.issueSuspect(xp.view)
		}
	} else {
		go xp.issueSuspect(xp.view)
	}
}
