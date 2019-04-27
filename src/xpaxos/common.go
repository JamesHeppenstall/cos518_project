package xpaxos

import (
	"sync"
	"labrpc"
	"crypto/rsa"
	"time"
	"testing"
)

const TIMEOUT = 5000 // Timeout period (in milliseconds) for Propose()
const DEBUG = 2      // Debugging (0 = None, 1 = Info, 2 = Debug)
const BITSIZE = 1024 // RSA private key bit size
const CLIENT = 0 // Client ID is always set to zero - DO NOT CHANGE

type Client struct {
	mu        sync.Mutex
	replicas  []*labrpc.ClientEnd
	timestamp int
	// Must include statistics for evaluation
}

type ClientRequest struct {
	MsgType   int
	Timestamp int
	Operation interface{}
	ClientId  int
}

type ReplicateReply struct {
	IsLeader bool
	Success  bool
}


const (
	REPLICATE = iota
	PREPARE   = iota
	COMMIT    = iota
	REPLY     = iota
	SUSPECT   = iota
	VIEWCHANGE = iota
	VCFINAL = iota
	NEWVIEW = iota
)

type XPaxos struct {
	mu               sync.Mutex
	persister        *Persister
	replicas         []*labrpc.ClientEnd
	synchronousGroup map[int]bool
	id               int
	view             int
	prepareSeqNum    int
	executeSeqNum    int
	prepareLog       []PrepareLogEntry
	commitLog        []CommitLogEntry
	privateKey       *rsa.PrivateKey
	publicKeys       map[int]*rsa.PublicKey
	suspectSet	     map[[32]byte]SuspectMessage
	vcSet            map[[32]byte]ViewChangeMessage
	netTimer	 	 <-chan time.Time
	netFlag          bool
	vcFlag           bool
	vcTimer			 <-chan time.Time
	receivedVcFinal  map[int]map[[32]byte]ViewChangeMessage
}

type Message struct {
	MsgType         int
	MsgDigest       [32]byte
	Signature       []byte
	PrepareSeqNum   int
	View            int
	ClientTimestamp int
	ServerId        int // XPaxos server that created the message
}

type PrepareLogEntry struct {
	Request ClientRequest
	Msg0    Message
}

type CommitLogEntry struct {
	Request ClientRequest
	Msg0    map[int]Message
	View int
}

type PrepareReply struct {
	MsgDigest [32]byte
	Signature []byte
	Success   bool
}

type CommitReply struct {
	MsgDigest [32]byte
	Signature []byte
	Success   bool
}


type SuspectMessage struct {
	MsgType int
	View int
	SenderId int
}

type ViewChangeMessage struct {
	MsgType int
	View int
	SenderId int
	CommitLog []CommitLogEntry
}

type VCFinalMessage struct {
	MsgType int
	View int
	SenderId int
	VCSet map[[32]byte]ViewChangeMessage
}

type NewViewMessage struct {
	MsgType int
	View int
	PrepareLog []PrepareLogEntry
}

type Reply struct {}


type config struct {
	mu          sync.Mutex
	t           *testing.T
	net         *labrpc.Network
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