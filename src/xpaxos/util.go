package xpaxos

import (
	"bytes"
	"crypto"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"log"
	"math/rand"
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
// ----------------------------- PERSISTER FUNCTIONS --------------------------
//
func (xp *XPaxos) persist() { // TODO
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(0)
	data := buf.Bytes()
	xp.persister.SaveXPaxosState(data)
}

func (xp *XPaxos) readPersist(data []byte) { // TODO
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(0)
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

func (xp *XPaxos) appendToCommitLog(request ClientRequest, msgMap map[int]Message) {
	commitEntry := CommitLogEntry{
		Request: request,
		Msg0:    msgMap,
		View:    xp.view}

	xp.commitLog = append(xp.commitLog, commitEntry)
}

func (xp *XPaxos) updatePrepareLog(prepareSeqNum int, request ClientRequest, msg Message) {
	prepareEntry := PrepareLogEntry{
		Request: request,
		Msg0:    msg}

	xp.prepareLog[prepareSeqNum] = prepareEntry
}

func (xp *XPaxos) compareLogs(prepareLog []PrepareLogEntry, commitLog []CommitLogEntry) bool {
	var commitEntryMsg Message
	var check1 bool
	var check2 bool
	var check3 bool

	if len(prepareLog) != len(commitLog) {
		return false
	} else {
		for seqNum, prepareEntry := range prepareLog {
			commitEntryMsg = commitLog[seqNum].Msg0[xp.getLeader()]

			check1 = (prepareEntry.Msg0.MsgDigest == commitEntryMsg.MsgDigest)
			check2 = (prepareEntry.Msg0.PrepareSeqNum == commitEntryMsg.PrepareSeqNum)
			check3 = (prepareEntry.Msg0.View == commitEntryMsg.View)

			if check1 != true || check2 != true || check3 != true {
				return false
			}
		}
		return true
	}
}
