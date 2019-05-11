package pbft

import (
	"bytes"
	"crypto"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"log"
)

//
// ------------------------------ DEBUG FUNCTIONS -----------------------------
//
func dPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 1 {
		log.Printf(format, a...)
	}
	return
}

func iPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 0 {
		log.Printf(format, a...)
	}
	return
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
	return
}

//
// ------------------------------ CRYPTO FUNCTIONS ----------------------------
//
func digest(msg interface{}) [32]byte { // Crypto message digest
	jsonBytes, _ := json.Marshal(msg)
	return sha256.Sum256(jsonBytes)
}

func generateKeys() (*rsa.PrivateKey, *rsa.PublicKey) { // Crypto RSA private/public key generation
	key, err := rsa.GenerateKey(crand.Reader, BITSIZE)
	checkError(err)
	return key, &key.PublicKey
}

func (pbft *Pbft) sign(msgDigest [32]byte) []byte { // Crypto message signature
	signature, err := rsa.SignPKCS1v15(crand.Reader, pbft.privateKey, crypto.SHA256, msgDigest[:])
	checkError(err)
	return signature
}

func (pbft *Pbft) verify(server int, msgDigest [32]byte, signature []byte) bool { // Crypto signature verification
	err := rsa.VerifyPKCS1v15(pbft.publicKeys[server], crypto.SHA256, msgDigest[:], signature)
	if err != nil {
		return false
	}
	return true
}

//
// ------------------------------ HELPER FUNCTIONS ----------------------------
//
func (pbft *Pbft) getLeader() int {
	return ((pbft.view - 1) % (len(pbft.replicas) - 1)) + 1
}

func (pbft *Pbft) generateSynchronousGroup(seed int64) {
	pbft.synchronousGroup = make(map[int]bool, 0)

	for i, _ := range pbft.replicas {
		if i != CLIENT {
			pbft.synchronousGroup[i] = true
		}
	}
}

func (pbft *Pbft) appendToPrepareLog(request ClientRequest, msg Message) PrepareLogEntry {
	pEDefault := PrepareLogEntry{}
	for request.Timestamp >= len(pbft.prepareLog) {
		pbft.prepareLog = append(pbft.prepareLog, pEDefault)
	}
	pE := pbft.prepareLog[request.Timestamp]

	if len(pE.Msg1) == 0 {
		msgMap := make(map[int]Message)
		msgMap[pbft.id] = msg
		prepareEntry := PrepareLogEntry{
			Request: request,
			Msg0:    msg,
			Msg1:    msgMap}

		pbft.prepareLog = append(pbft.prepareLog, prepareEntry)
	}
	msgMapCopy := make(map[int]Message)
	prepareEntryCopy := PrepareLogEntry{
		Request: request,
		Msg0:    msg,
		Msg1:    msgMapCopy}
	return prepareEntryCopy
}

func (pbft *Pbft) addToPrepareLog(prepareLog PrepareLogEntry) bool {
	sender := prepareLog.Hop
	if sender == 0 {
		sender = prepareLog.Msg0.SenderId
	}
	pEDefault := PrepareLogEntry{}
	for prepareLog.Msg0.PrepareSeqNum >= len(pbft.prepareLog) {
		pbft.prepareLog = append(pbft.prepareLog, pEDefault)
	}
	if prepareLog.Msg0.PrepareSeqNum < len(pbft.prepareLog) {
		pE := pbft.prepareLog[prepareLog.Msg0.PrepareSeqNum]

		if len(pE.Msg1) == 0 {
			msgMap := make(map[int]Message)
			msgMap[sender] = prepareLog.Msg0
			prepareEntry := PrepareLogEntry{
				Request: prepareLog.Request,
				Msg0:    prepareLog.Msg0,
				Msg1:    msgMap}
			pbft.prepareLog[prepareLog.Msg0.PrepareSeqNum] = prepareEntry
			return false
		} else {
			pE.Msg1[sender] = prepareLog.Msg0
			if len(pE.Msg1) >= 2*(len(pbft.replicas)-2)/3 {
				return true
			}
			return false
		}
	}
	return false
}

func (pbft *Pbft) addToCommitLog(cmsg CommitMessage) bool {
	cEDefault := CommitLogEntry{}
	for cmsg.Msg.PrepareSeqNum >= len(pbft.commitLog) {
		pbft.commitLog = append(pbft.commitLog, cEDefault)
	}
	if cmsg.Msg.PrepareSeqNum < len(pbft.commitLog) {
		cE := pbft.commitLog[cmsg.Msg.PrepareSeqNum]

		if len(cE.Msg1) == 0 {
			msgMap := make(map[int]Message)
			msgMap[cmsg.Msg.SenderId] = cmsg.Msg
			commitEntry := CommitLogEntry{
				Request: cmsg.Request,
				Msg0:    cmsg.Msg,
				Msg1:    msgMap}
			pbft.commitLog[cmsg.Msg.PrepareSeqNum] = commitEntry
			return false
		} else {
			cE.Msg1[cmsg.Msg.SenderId] = cmsg.Msg
			if len(cE.Msg1) >= 2*(len(pbft.replicas)-2)/3 {
				return true
			}
			return false
		}
	}
	return false
}

func (pbft *Pbft) appendToCommitLog(request ClientRequest, msg Message, msgMap map[int]Message) {
	commitEntry := CommitLogEntry{
		Request: request,
		Msg0:    msg,
		Msg1:    msgMap,
		View:    pbft.view}

	pbft.commitLog = append(pbft.commitLog, commitEntry)
}

func (pbft *Pbft) updatePrepareLog(seqNum int, request ClientRequest, msg Message) {
	prepareEntry := PrepareLogEntry{
		Request: request,
		Msg0:    msg}

	pbft.prepareLog[seqNum] = prepareEntry
}

func (pbft *Pbft) compareLogs(prepareLog []PrepareLogEntry, commitLog []CommitLogEntry) bool {
	var commitEntryMsg0 Message
	var check1 int
	var check2 bool
	var check3 bool

	if len(prepareLog) != len(commitLog) {
		return false
	} else {
		for seqNum, prepareEntry := range prepareLog {
			commitEntryMsg0 = commitLog[seqNum].Msg0

			check1 = bytes.Compare(prepareEntry.Msg0.MsgDigest[:], commitEntryMsg0.MsgDigest[:])
			check2 = (prepareEntry.Msg0.PrepareSeqNum == commitEntryMsg0.PrepareSeqNum)
			check3 = (prepareEntry.Msg0.ClientTimestamp == commitEntryMsg0.ClientTimestamp)

			if check1 != 0 || check2 != true || check3 != true {
				return false
			}
		}
		return true
	}
}

//
// ------------------------------ TEST FUNCTIONS ------------------------------
//
func comparePrepareSeqNums(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		prepareSeqNum := cfg.pbftServers[i].prepareSeqNum
		if cfg.pbftServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if i != j && cfg.pbftServers[i].synchronousGroup[j] == true && cfg.pbftServers[j].prepareSeqNum != prepareSeqNum {
					cfg.t.Fatal("Invalid prepare sequence numbers!")
				}
			}
		}
	}
}

func compareExecuteSeqNums(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		executeSeqNum := cfg.pbftServers[i].executeSeqNum
		if cfg.pbftServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if i != j && cfg.pbftServers[i].synchronousGroup[j] == true && cfg.pbftServers[j].executeSeqNum != executeSeqNum {
					cfg.t.Fatal("Invalid execute sequence numbers!")
				}
			}
		}
	}
}

func comparePrepareLogEntries(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		prepareLogDigest := digest(cfg.pbftServers[i].prepareLog)
		if cfg.pbftServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if cfg.pbftServers[i].synchronousGroup[j] == true && digest(cfg.pbftServers[j].prepareLog) != prepareLogDigest {
					cfg.t.Fatal("Invalid prepare logs!")
				}
			}
		}
	}
}

func compareCommitLogEntries(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		commitLogDigest := digest(cfg.pbftServers[i].commitLog)
		if cfg.pbftServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if cfg.pbftServers[i].synchronousGroup[j] == true && digest(cfg.pbftServers[j].commitLog) != commitLogDigest {
					cfg.t.Fatal("Invalid commit logs!")
				}
			}
		}
	}
}

func getCurrentView(cfg *config) int {
	numCurrent := 0
	currentView := 0

	for _, pbftServer := range cfg.pbftServers[1:] {
		if pbftServer.view > currentView {
			numCurrent = 1
			currentView = pbftServer.view
		} else if pbftServer.view == currentView {
			numCurrent++
		}
	}

	if numCurrent < (len(cfg.pbftServers)+1)/2 {
		cfg.t.Fatal("Invalid current view (no majority)!")
	}

	return currentView
}
