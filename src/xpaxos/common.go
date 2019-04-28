package xpaxos

import (
	"crypto/rsa"
	"network"
	"sync"
	"testing"
	"time"
)

const DEBUG = 1      // Debugging (0 = None, 1 = Info, 2 = Debug)
const CLIENT = 0     // Client ID is always set to zero - DO NOT CHANGE
const TIMEOUT = 5000 // Client timeout period (in milliseconds) - SHOULD ONLY HAPPEN IN ANARCHY
const BITSIZE = 1024 // RSA private key bit size

const ( // RPC message types for common case and view change protocols
	REPLICATE  = iota
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
	xpServers   []*XPaxos
	client      *Client
	connected   []bool // Whether each server is on the net
	saved       []*Persister
	endnames    [][]string // The port file names each sends to
	privateKeys map[int]*rsa.PrivateKey
	publicKeys  map[int]*rsa.PublicKey
}

type Client struct {
	mu        sync.Mutex
	replicas  []*network.ClientEnd
	timestamp int
	vcCh      chan bool
	// Must include statistics for evaluation
}

type XPaxos struct {
	mu               sync.Mutex
	persister        *Persister
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
	suspectSet       map[[32]byte]SuspectMessage
	vcSet            map[[32]byte]ViewChangeMessage
	netFlag          bool // Flag to tell if netTimer is still valid
	netTimer         <-chan time.Time
	vcFlag           bool // Flag to tell if vcTimer is still valid
	vcTimer          <-chan time.Time
	receivedVCFinal  map[int]map[[32]byte]ViewChangeMessage
}

type PrepareLogEntry struct {
	Request ClientRequest
	Msg0    Message
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

type Reply struct {
	MsgDigest  [32]byte
	Signature  []byte
	Success    bool
	IsLeader   bool
	Suspicious bool
}

type SuspectMessage struct {
	MsgType   int
	MsgDigest [32]byte
	Signature []byte
	View      int
	SenderId  int
}

type ViewChangeMessage struct {
	MsgType   int
	MsgDigest [32]byte
	Signature []byte
	View      int
	SenderId  int
	CommitLog []CommitLogEntry
}

type VCFinalMessage struct {
	MsgType   int
	MsgDigest [32]byte
	Signature []byte
	View      int
	SenderId  int
	VCSet     map[[32]byte]ViewChangeMessage
}

type NewViewMessage struct {
	MsgType    int
	MsgDigest  [32]byte
	Signature  []byte
	View       int
	PrepareLog []PrepareLogEntry
}
