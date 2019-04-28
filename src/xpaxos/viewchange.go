package xpaxos

import (
	"network"
	"time"
)

//
// -------------------------------- SUSPECT RPC -------------------------------
//
func (xp *XPaxos) sendSuspect(server int, msg SuspectMessage, reply *Reply) bool {
	dPrintf("Suspect: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.Suspect", msg, reply, xp.id)
}

func (xp *XPaxos) issueSuspect(view int) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != view {
		return
	}

	msg := SuspectMessage{
		MsgType:  SUSPECT,
		View:     xp.view,
		SenderId: xp.id}

	reply := &Reply{}

	for server, _ := range xp.replicas {
		if server != CLIENT {
			go xp.sendSuspect(server, msg, reply)
		}
	}
}

func (xp *XPaxos) forwardSuspect(msg SuspectMessage) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != msg.View+1 {
		return
	}

	reply := &Reply{}

	for server, _ := range xp.replicas {
		if server != CLIENT {
			go xp.sendSuspect(server, msg, reply)
		}
	}
}

func (xp *XPaxos) Suspect(msg SuspectMessage, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	_, ok := xp.suspectSet[digest(msg)]

	if xp.view <= msg.View && ok == false {
		xp.suspectSet[digest(msg)] = msg

		xp.view = msg.View+1
		go xp.forwardSuspect(msg)

		xp.generateSynchronousGroup(int64(xp.view))
		xp.vcSet = make(map[[32]byte]ViewChangeMessage, 0)
		xp.receivedVCFinal = make(map[int]map[[32]byte]ViewChangeMessage, 0)

		go xp.issueViewChange(xp.view)

		if len(xp.synchronousGroup) > 0 {
			xp.netFlag = false
			xp.netTimer = time.NewTimer(2 * network.DELTA * time.Millisecond).C
		}
	}
}

//
// ------------------------------ VIEW-CHANGE RPC -----------------------------
//
func (xp *XPaxos) sendViewChange(server int, msg ViewChangeMessage, reply *Reply) bool {
	dPrintf("ViewChange: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.ViewChange", msg, reply, xp.id)
}

func (xp *XPaxos) issueViewChange(view int) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != view {
		return
	}

	msg := ViewChangeMessage{
		MsgType:   VIEWCHANGE,
		View:      xp.view,
		SenderId:  xp.id,
		CommitLog: xp.commitLog}

	reply := &Reply{}

	for server, _ := range xp.synchronousGroup {
		go xp.sendViewChange(server, msg, reply)
	}
}

func (xp *XPaxos) ViewChange(msg ViewChangeMessage, reply *Reply) {
	xp.mu.Lock()
	if xp.view == msg.View {
		xp.vcSet[digest(msg)] = msg

		if len(xp.vcSet) == len(xp.replicas)-1 {
			xp.setVCTimer()
			go xp.issueVCFinal(xp.view)
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
	}
	xp.mu.Unlock()
}

//
// ------------------------------- VC-FINAL RPC -------------------------------
//
func (xp *XPaxos) sendVCFinal(server int, msg VCFinalMessage, reply *Reply) bool {
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

	msg := VCFinalMessage{
		MsgType:  VCFINAL,
		View:     xp.view,
		SenderId: xp.id,
		VCSet:    vcSetCopy}

	reply := &Reply{}

	for server, _ := range xp.synchronousGroup {
		go xp.sendVCFinal(server, msg, reply)
	}
}

func (xp *XPaxos) VCFinal(msg VCFinalMessage, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != msg.View {
		return
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

				go xp.issueNewView(xp.view)
			}
		}
	}
}

//
// -------------------------------- NEW-VIEW RPC ------------------------------
//
func (xp *XPaxos) sendNewView(server int, msg NewViewMessage, reply *Reply) bool {
	dPrintf("NewView: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.NewView", msg, reply, xp.id)
}

func (xp *XPaxos) issueNewView(view int) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != view {
		return
	}

	msg := NewViewMessage{
		MsgType:    NEWVIEW,
		View:       xp.view,
		PrepareLog: xp.prepareLog}

	reply := &Reply{}

	for server, _ := range xp.synchronousGroup {
		go xp.sendNewView(server, msg, reply)
	}
}

func (xp *XPaxos) NewView(msg NewViewMessage, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	if xp.view != msg.View {
		return
	}

	xp.vcFlag = true

	if xp.compareLogs(msg.PrepareLog, xp.commitLog) {
		xp.prepareLog = msg.PrepareLog
		xp.prepareSeqNum = len(xp.prepareLog)
		xp.executeSeqNum = len(xp.commitLog)

		xp.suspectSet = make(map[[32]byte]SuspectMessage, 0)
		xp.vcSet = make(map[[32]byte]ViewChangeMessage, 0)
		xp.receivedVCFinal = make(map[int]map[[32]byte]ViewChangeMessage, 0)

		if xp.id == xp.getLeader() {
			go xp.issueConfirmVC()
		}
	} else {
		go xp.issueSuspect(xp.view)
	}
}
