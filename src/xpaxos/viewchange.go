package xpaxos

import (
	"labrpc"
	"time"
)

//
// -------------------------------- SUSPECT RPC -------------------------------
//
func (xp *XPaxos) sendSuspect(server int, msg SuspectMessage, reply *Reply) bool {
	dPrintf("Suspect: from XPaxos server (%d) to XPaxos server (%d) %d\n", xp.id, server, xp.view)
	return xp.replicas[server].Call("XPaxos.Suspect", msg, reply, xp.id)
}

func (xp *XPaxos) issueSuspect() {
	xp.mu.Lock()
	defer xp.mu.Unlock()

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

	reply := &Reply{}

	for server, _ := range xp.replicas {
		if server != CLIENT {
			go xp.sendSuspect(server, msg, reply)
		}
	}
}

func (xp *XPaxos) Suspect(msg SuspectMessage, reply *Reply) {
	xp.mu.Lock()
	_, ok := xp.suspectSet[digest(msg)]

	if xp.view <= msg.View && ok == false {
		xp.suspectSet[digest(msg)] = msg
		xp.mu.Unlock()
		
		go xp.forwardSuspect(msg)

		xp.mu.Lock()
		if xp.view == msg.View {
			xp.view++	
		} else {
			xp.view = msg.View
		}

		xp.vcSet = make(map[[32]byte]ViewChangeMessage, 0)
		xp.receivedVCFinal = make(map[int]map[[32]byte]ViewChangeMessage, 0)

		xp.generateSynchronousGroup(int64(xp.view))
		xp.mu.Unlock()

		go xp.issueViewChange()

		xp.mu.Lock()

		if len(xp.synchronousGroup) > 0 {
			xp.netFlag = false
			xp.netTimer = time.NewTimer(2 * labrpc.DELTA * time.Millisecond).C
		}
	}
	xp.mu.Unlock()
}

//
// ------------------------------ VIEW-CHANGE RPC -----------------------------
//
func (xp *XPaxos) sendViewChange(server int, msg ViewChangeMessage, reply *Reply) bool {
	dPrintf("ViewChange: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.ViewChange", msg, reply, xp.id)
}

func (xp *XPaxos) issueViewChange() {
	xp.mu.Lock()

	msg := ViewChangeMessage{
		MsgType:   VIEWCHANGE,
		View:      xp.view,
		SenderId:  xp.id,
		CommitLog: xp.commitLog}

	reply := &Reply{}

	for server, _ := range xp.synchronousGroup {
		xp.mu.Unlock()

		go xp.sendViewChange(server, msg, reply)
		xp.mu.Lock()
	}
	xp.mu.Unlock()
}

func (xp *XPaxos) ViewChange(msg ViewChangeMessage, reply *Reply) {
	xp.mu.Lock()
	
	if xp.view <= msg.View {
		xp.vcSet[digest(msg)] = msg
		if len(xp.vcSet) == len(xp.replicas)-1 {
			xp.netFlag = true
			xp.vcFlag = false
			xp.vcTimer = time.NewTimer(2 * labrpc.DELTA * time.Millisecond).C

			go xp.issueVCFinal()
			go func(xp *XPaxos) {
				<-xp.vcTimer

				xp.mu.Lock()
				if xp.vcFlag == false {
					go xp.issueSuspect()
				}
				xp.mu.Unlock()

			}(xp)
			return
		}
		xp.mu.Unlock()

		<-xp.netTimer

		xp.mu.Lock()
		if xp.netFlag == false && len(xp.vcSet) >= (len(xp.replicas)+1)/2 {
			xp.netFlag = true
			xp.vcFlag = false
			xp.vcTimer = time.NewTimer(5 * labrpc.DELTA * time.Millisecond).C

			go xp.issueVCFinal()

			go func(xp *XPaxos) {
				<-xp.vcTimer

				if xp.vcFlag == false {
					iPrintf("VCTIMER TIMEOUT")
					go xp.issueSuspect()
				}
			}(xp)
		} else if xp.netFlag == false {
			xp.vcFlag = true 
			go xp.issueSuspect()
		}
	}
	xp.mu.Unlock()

}

//
// -------------------------------- VC-FINAL RPC ------------------------------
//
func (xp *XPaxos) sendVCFinal(server int, msg VCFinalMessage, reply *Reply) bool {
	dPrintf("VCFinal: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.VCFinal", msg, reply, xp.id)
}

func (xp *XPaxos) issueVCFinal() {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	sendVcSet := make(map[[32]byte]ViewChangeMessage)

	for k, v := range xp.vcSet {
		sendVcSet[k] = v
	}
	msg := VCFinalMessage{
		MsgType:  VCFINAL,
		View:     xp.view,
		SenderId: xp.id,
		VCSet:    sendVcSet}

	reply := &Reply{}
	for server, _ := range xp.synchronousGroup {
		go xp.sendVCFinal(server, msg, reply)
	}
}

func (xp *XPaxos) VCFinal(msg VCFinalMessage, reply *Reply) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

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
						PrepareSeqNum:   sn+1,
						View:            xp.view,
						ClientTimestamp: msg0.ClientTimestamp,
						SenderId:        msg0.SenderId}

					if sn < len(xp.prepareLog)-1 { 
						xp.updatePrepareLog(sn+1, request, newMsg0)
					} else {
						xp.appendToPrepareLog(request, newMsg0)
					}
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
	iPrintf("NewView: from XPaxos server (%d) to XPaxos server (%d)\n", xp.id, server)
	return xp.replicas[server].Call("XPaxos.NewView", msg, reply, xp.id)
}

func (xp *XPaxos) issueNewView() {
	xp.mu.Lock()
	defer xp.mu.Unlock()

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

	iPrintf("%v %v\n", msg.PrepareLog, xp.commitLog)

	if xp.compareLogs(msg.PrepareLog, xp.commitLog) {
		xp.prepareLog = msg.PrepareLog
		xp.prepareSeqNum = len(xp.prepareLog) - 1
		xp.executeSeqNum = len(xp.commitLog) - 1
		xp.vcFlag = true
	}// else {
	//	go xp.issueSuspect()
	//}
}
