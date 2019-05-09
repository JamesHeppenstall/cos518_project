package xpaxos

import (
	"bytes"
	"crypto"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"log"
	"math/rand"
	"network"
	"time"
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

func (xp *XPaxos) sign(msgDigest [32]byte) []byte { // Crypto message signature
	signature, err := rsa.SignPKCS1v15(crand.Reader, xp.privateKey, crypto.SHA256, msgDigest[:])
	checkError(err)
	return signature
}

func (xp *XPaxos) verify(server int, msgDigest [32]byte, signature []byte) bool { // Crypto signature verification
	err := rsa.VerifyPKCS1v15(xp.publicKeys[server], crypto.SHA256, msgDigest[:], signature)
	if err != nil {
		return false
	}
	return true
}

//
// ------------------------------ HELPER FUNCTIONS ----------------------------
//
func (xp *XPaxos) getLeader() int {
	return ((xp.view - 1) % (len(xp.replicas) - 1)) + 1
}

func (xp *XPaxos) generateSynchronousGroup(seed int64) {
	r := rand.New(rand.NewSource(seed))
	numAdded := 0

	xp.synchronousGroup = make(map[int]bool, 0)
	xp.synchronousGroup[xp.getLeader()] = true

	for _, server := range r.Perm(len(xp.replicas)) {
		if server != CLIENT && server != xp.getLeader() && numAdded < (len(xp.replicas)-1)/2 {
			xp.synchronousGroup[server] = true
			numAdded++
		}
	}

	if xp.synchronousGroup[xp.id] != true {
		xp.synchronousGroup = make(map[int]bool, 0)
	}
}

func (xp *XPaxos) appendToPrepareLog(request ClientRequest, msg Message) PrepareLogEntry {
	prepareEntry := PrepareLogEntry{
		Request: request,
		Msg0:    msg}

	xp.prepareLog = append(xp.prepareLog, prepareEntry)
	return prepareEntry
}

func (xp *XPaxos) appendToCommitLog(request ClientRequest, msg Message, msgMap map[int]Message) {
	commitEntry := CommitLogEntry{
		Request: request,
		Msg0:    msg,
		Msg1:    msgMap,
		View:    xp.view}

	xp.commitLog = append(xp.commitLog, commitEntry)
}

func (xp *XPaxos) updatePrepareLog(seqNum int, request ClientRequest, msg Message) {
	prepareEntry := PrepareLogEntry{
		Request: request,
		Msg0:    msg}

	xp.prepareLog[seqNum] = prepareEntry
}

func (xp *XPaxos) compareLogs(prepareLog []PrepareLogEntry, commitLog []CommitLogEntry) bool {
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

func (xp *XPaxos) setVCTimer() {
	oldView := xp.view

	xp.netFlag = true
	xp.vcFlag = false
	xp.vcTimer = time.NewTimer(3 * network.DELTA * time.Millisecond).C

	go func(xp *XPaxos, oldView int) {
		<-xp.vcTimer

		xp.mu.Lock()
		if xp.vcFlag == false && xp.view == oldView {
			dPrintf("Timeout: XPaxos.setVCTimer: XPaxos server (%d)\n", xp.id)
			go xp.issueSuspect(xp.view)
		}
		xp.mu.Unlock()
	}(xp, oldView)
}

func (xp *XPaxos) issueConfirmVC() bool {
	dPrintf("ConfirmVC: from XPaxos server (%d) to client server (%d)\n", xp.id, CLIENT)
	return xp.replicas[CLIENT].Call("Client.ConfirmVC", Message{}, &Reply{}, xp.id)
}

//
// ------------------------------ TEST FUNCTIONS ------------------------------
//
func comparePrepareSeqNums(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		prepareSeqNum := cfg.xpServers[i].prepareSeqNum
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if i != j && cfg.xpServers[i].synchronousGroup[j] == true && cfg.xpServers[j].prepareSeqNum != prepareSeqNum {
					if cfg.xpServers[i].vcInProgress == false && cfg.xpServers[j].vcInProgress == false {
						iPrintf("%d %d %d %d\n", i, j, cfg.xpServers[i].prepareSeqNum, cfg.xpServers[j].prepareSeqNum)
						cfg.t.Fatal("Invalid prepare sequence numbers!")
					}
				}
			}
		}
	}
}

func compareExecuteSeqNums(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		executeSeqNum := cfg.xpServers[i].executeSeqNum
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if i != j && cfg.xpServers[i].synchronousGroup[j] == true && cfg.xpServers[j].executeSeqNum != executeSeqNum {
					if cfg.xpServers[i].vcInProgress == false && cfg.xpServers[j].vcInProgress == false {
						iPrintf("%d %d %d %d\n", i, j, cfg.xpServers[i].executeSeqNum, cfg.xpServers[j].executeSeqNum)
						cfg.t.Fatal("Invalid execute sequence numbers!")
					}
				}
			}
		}
	}
}

func comparePrepareLogEntries(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		prepareLogDigest := digest(cfg.xpServers[i].prepareLog)
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if cfg.xpServers[i].synchronousGroup[j] == true && digest(cfg.xpServers[j].prepareLog) != prepareLogDigest {
					if cfg.xpServers[i].vcInProgress == false && cfg.xpServers[j].vcInProgress == false {
						cfg.t.Fatal("Invalid prepare logs!")
					}
				}
			}
		}
	}
}

func compareCommitLogEntries(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		commitLogDigest := digest(cfg.xpServers[i].commitLog)
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if cfg.xpServers[i].synchronousGroup[j] == true && digest(cfg.xpServers[j].commitLog) != commitLogDigest {
					if cfg.xpServers[i].vcInProgress == false && cfg.xpServers[j].vcInProgress == false {
						if compareCommitLogEntriesChecker(cfg.xpServers[i].commitLog, cfg.xpServers[j].commitLog) == false {
							cfg.t.Fatal("Invalid commit logs!")
						}
					}
				}
			}
		}
	}
}

func compareCommitLogEntriesChecker(commitLog1 []CommitLogEntry, commitLog2 []CommitLogEntry) bool {
	if len(commitLog1) != len(commitLog2) {
		return false
	} else {
		for i, commitEntry := range commitLog1 {
			if digest(commitEntry.Request) != digest(commitLog2[i].Request) {
				return false
			}
			if digest(commitEntry.Msg0) != digest(commitLog2[i].Msg0) {
				return false
			}
			if commitEntry.View != commitLog2[i].View {
				return false
			}
		}
	}
	return true
}

func getCurrentView(cfg *config) int {
	numCurrent := 0
	currentView := 0

	for _, xpServer := range cfg.xpServers[1:] {
		if xpServer.view > currentView {
			numCurrent = 1
			currentView = xpServer.view
		} else if xpServer.view == currentView {
			numCurrent++
		}
	}

	if numCurrent < (len(cfg.xpServers)+1)/2 {
		cfg.t.Fatal("Invalid current view (no majority)!")
	}

	return currentView
}
