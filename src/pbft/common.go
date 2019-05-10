package pbft

import (
	"crypto/rsa"
	"network"
	"sync"
	"testing"
)

const DEBUG = 0       // Debugging (0 = None, 1 = Info, 2 = Debug)
const CLIENT = 0      // Client ID is always set to zero - DO NOT CHANGE
const TIMEOUT = 500 // Client timeout period (in milliseconds)
const WAIT = false     // If false, client times out after TIMEOUT milliseconds; if true, client never times out
const BITSIZE = 1024  // RSA private key bit size

const ( // RPC message types for common case and view change protocols
	REPLICATE  = iota
	PREPREPARE = iota
	PREPARE    = iota
	COMMIT     = iota
	REPLY      = iota
	SUSPECT    = iota
	VIEWCHANGE = iota
	VCFINAL    = iota
	NEWVIEW    = iota
)

type config struct {
	mu          sync.Mutex
	t           *testing.T
	net         *network.Network
	n           int   // Total number of client and XPaxos servers
	done        int32 // Tell internal threads to die
	pbftServers []*Pbft
	client      *Client
	connected   []bool // Whether each server is on the net
	endnames    [][]string // The port file names each sends to
	privateKeys map[int]*rsa.PrivateKey
	publicKeys  map[int]*rsa.PublicKey
}

type Client struct {
	mu        sync.Mutex
	replicas  []*network.ClientEnd
	timestamp int
	committed int
	vcCh      chan bool
	// Must include statistics for evaluation
	replyMap  []map[int]bool
}

type Pbft struct {
	mu               sync.Mutex
	replicas         []*network.ClientEnd
	synchronousGroup map[int]bool
	id               int
	view             int
	prepareSeqNum    int
	executeSeqNum    int
	prepareLog       []PrepareLogEntry
	commitLog        []CommitLogEntry
	privateKey       *rsa.PrivateKey
	publicKeys       map[int]*rsa.PublicKey
}

type PrepareLogEntry struct {
	Request ClientRequest
	Msg1    map[int]Message
	Msg0    Message
	Hop 	int
}

type CommitLogEntry struct {
	Request ClientRequest
	Msg0    Message
	Msg1    map[int]Message
	View    int
}

type ClientRequest struct {
	MsgType   int
	Timestamp int
	Operation interface{}
	ClientId  int
}

type Message struct {
	MsgType         int
	MsgDigest       [32]byte
	Signature       []byte
	PrepareSeqNum   int
	View            int
	ClientTimestamp int
	SenderId        int
}

type CommitMessage struct {
	Msg Message
	Request ClientRequest
}

type Reply struct {
	MsgDigest  [32]byte
	Signature  []byte
	Success    bool
	IsLeader   bool
	Suspicious bool
}

type ClientReply struct {
	Commiter  int 
	Timestamp int
}
