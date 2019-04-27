package xpaxos

import (
	"labrpc"
	"time"
)

//
// -------------------------------- SUSPECT RPC -------------------------------
//
func (xp *XPaxos) sendSuspect(server int, msg SuspectMessage, reply *Reply) bool {
	dPrintf("Suspect: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.Suspect", msg, reply, xp.id)
}

func (xp *XPaxos) issueSuspect() {
	msg := SuspectMessage{
		MsgType:  SUSPECT,
		View:     xp.view,
		SenderId: xp.id}

	for server, _ := range xp.replicas {
		if server != CLIENT {
			go xp.sendSuspect(server, msg, nil)
		}
	}
}

func (xp *XPaxos) Suspect(msg SuspectMessage, reply *Reply) {
	_, ok := xp.suspectSet[digest(msg)]

	if xp.view <= msg.View && ok == false {
		xp.suspectSet[digest(msg)] = msg
		go xp.issueSuspect()

		xp.view++
		xp.generateSynchronousGroup(int64(xp.getLeader()))

		go xp.issueViewChange()

		if len(xp.synchronousGroup) > 0 {
			xp.netFlag = false
			xp.netTimer = time.NewTimer(2 * labrpc.DELTA * time.Millisecond).C
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

func (xp *XPaxos) issueViewChange() {
	msg := ViewChangeMessage{
		MsgType:   VIEWCHANGE,
		View:      xp.view,
		SenderId:  xp.id,
		CommitLog: xp.commitLog}

	for server, _ := range xp.synchronousGroup {
		go xp.sendViewChange(server, msg, nil)
	}
}

func (xp *XPaxos) ViewChange(msg ViewChangeMessage, reply *Reply) {
	xp.vcSet[digest(msg)] = msg

	if len(xp.vcSet) == len(xp.replicas)-1 {
		xp.netFlag = true
		go xp.issueVCFinal()

		xp.vcFlag = false
		xp.vcTimer = time.NewTimer(TIMEOUT * time.Millisecond).C

		go func(xp *XPaxos) {
			<-xp.vcTimer

			if xp.vcFlag == false {
				go xp.issueSuspect()
			}
		}(xp)
	}

	<-xp.netTimer

	if xp.netFlag == false && len(xp.vcSet) >= (len(xp.replicas)+1)/2 {
		xp.netFlag = true
		go xp.issueVCFinal()

		xp.vcFlag = false
		xp.vcTimer = time.NewTimer(TIMEOUT * time.Millisecond).C

		go func(xp *XPaxos) {
			<-xp.vcTimer

			if xp.vcFlag == false {
				go xp.issueSuspect()
			}
		}(xp)
	}
}

//
// -------------------------------- VC-FINAL RPC ------------------------------
//
func (xp *XPaxos) sendVCFinal(server int, msg VCFinalMessage, reply *Reply) bool {
	dPrintf("VCFinal: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.VCFinal", msg, reply, xp.id)
}

func (xp *XPaxos) issueVCFinal() {
	msg := VCFinalMessage{
		MsgType:  VCFINAL,
		View:     xp.view,
		SenderId: xp.id,
		VCSet:    xp.vcSet}

	for server, _ := range xp.synchronousGroup {
		go xp.sendVCFinal(server, msg, nil)
	}
}

func (xp *XPaxos) VCFinal(msg VCFinalMessage, reply *Reply) {
	if xp.synchronousGroup[msg.SenderId] == true {
		xp.receivedVCFinal[msg.SenderId] = msg.VCSet

		if len(xp.receivedVCFinal) >= len(xp.synchronousGroup) {
			for _, msg := range msg.VCSet {
				xp.vcSet[digest(msg)] = msg
			}

			for _, msg := range xp.vcSet {
				for sn, _ := range msg.CommitLog {
					if len(xp.commitLog) <= sn {
						xp.commitLog = append(xp.commitLog, msg.CommitLog[sn])
					} else {
						if xp.commitLog[sn].View < msg.CommitLog[sn].View {
							xp.commitLog[sn] = msg.CommitLog[sn]
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

				for sn, _ := range xp.commitLog {
					request = xp.commitLog[sn].Request
					msg0 = xp.commitLog[sn].Msg0[xp.id]

					msgDigest = digest(request)
					signature = xp.sign(msgDigest)

					newMsg0 = Message{
						MsgType:         PREPARE,
						MsgDigest:       msgDigest,
						Signature:       signature,
						PrepareSeqNum:   sn,
						View:            xp.view,
						ClientTimestamp: msg0.ClientTimestamp,
						SenderId:        msg0.SenderId}

					xp.updatePrepareLog(sn, request, newMsg0)
				}

				go xp.issueNewView()
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

func (xp *XPaxos) issueNewView() {
	msg := NewViewMessage{
		MsgType:    NEWVIEW,
		View:       xp.view,
		PrepareLog: xp.prepareLog}

	for server, _ := range xp.synchronousGroup {
		go xp.sendNewView(server, msg, nil)
	}
}

func (xp *XPaxos) NewView(msg NewViewMessage, reply *Reply) {
	if xp.compareLogs(msg.PrepareLog, xp.commitLog) {
		xp.prepareLog = msg.PrepareLog
		xp.prepareSeqNum = len(xp.prepareLog) - 1
		xp.executeSeqNum = len(xp.commitLog) - 1
		xp.vcFlag = true
	} else {
		go xp.issueSuspect()
	}
}
