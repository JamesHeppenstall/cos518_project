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

const DEBUG = 2 // Debugging (0 = None, 1 = Info, 2 = Debug)

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
// ------------------------------ HELPER FUNCTIONS ----------------------------
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

func (xp *XPaxos) getState() (int, bool) {
	xp.mu.Lock()
	defer xp.mu.Unlock()

	isLeader := false
	view := xp.view

	if xp.id == view {
		isLeader = true
	}

	return view, isLeader
}

func (xp *XPaxos) generateSynchronousGroup(seed int64) {
	r := rand.New(rand.NewSource(seed))
	numAdded := 0

	xp.synchronousGroup[xp.view] = true

	for _, server := range r.Perm(len(xp.replicas)) {
		if server != CLIENT && server != xp.view && numAdded < (len(xp.replicas)-1)/2 {
			xp.synchronousGroup[server] = true
			numAdded++
		}
	}

	if xp.synchronousGroup[xp.id] != true {
		xp.synchronousGroup = make(map[int]bool, 0)
	}
}

func (xp *XPaxos) persist() {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(0)
	data := buf.Bytes()
	xp.persister.SaveXPaxosState(data)
}

func (xp *XPaxos) readPersist(data []byte) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(0)
}
