package xpaxos

import "labrpc"

//import "log"
import "sync"
import "testing"
import "runtime"
import crand "crypto/rand"
import "encoding/base64"
import "sync/atomic"


func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *labrpc.Network
	n         int
	done      int32 // Tell internal threads to die
	xpServers []*XPaxos
	client    *Client
	applyErr  []string // From apply channel readers
	connected []bool   // Whether each server is on the net
	saved     []*Persister
	endnames  [][]string    // The port file names each sends to
	logs      []map[int]int // Copy of each server's committed entries
}

func MakeConfig(t *testing.T, n int, unreliable bool) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.xpServers = make([]*XPaxos, cfg.n)
	cfg.client = &Client{}
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]int, cfg.n)

	cfg.setUnreliable(unreliable)

	cfg.net.LongDelays(false)


	cfg.logs[0] = map[int]int{}
	cfg.startClient(0)

	// Create a full set of XPaxos servers
	for i := 1; i < cfg.n; i++ {
		cfg.logs[i] = map[int]int{}
		cfg.start1(i)
	}

	// Connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	cfg.client.propose()

	return cfg
}

// Shut down an XPaxos server but save its persistent state
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // Disable client connections to the server

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// A fresh persister, in case old instance continues to update the Persister
	// Copy old persister's content so that we always pass Make() the last persisted state
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	xp := cfg.xpServers[i]
	if xp != nil {
		cfg.mu.Unlock()
		xp.Kill()
		cfg.mu.Lock()
		cfg.xpServers[i] = nil
	}

	if cfg.saved[i] != nil {
		xpLog := cfg.saved[i].ReadXPaxosState()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].SaveXPaxosState(xpLog)
	}
}

// Start or re-start an XPaxos server
// If one already exists, "kill" it first
// Allocate new outgoing port file names, and a new state persister, to isolate previous instance of
// this server. since we cannot really kill it
func (cfg *config) start1(i int) {
	cfg.crash1(i)

	// A fresh set of outgoing ClientEnd names so that old crashed instance's ClientEnds can't send
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// A fresh set of ClientEnds
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	// A fresh persister, so old instance doesn't overwrite new instance's persisted state
	// Copy old persister's content so that we always pass Make() the last persisted state
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	xp := Make(ends, i, cfg.saved[i])

	cfg.mu.Lock()
	cfg.xpServers[i] = xp
	cfg.mu.Unlock()

	svc := labrpc.MakeService(xp)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

// Start or re-start an XPaxos Client
// If one already exists, "kill" it first
// Allocate new outgoing port file names, and a new state persister, to isolate previous instance of
// this server. since we cannot really kill it
func (cfg *config) startClient(i int) {
	cfg.crashClient(i)

	// A fresh set of outgoing ClientEnd names so that old crashed instance's ClientEnds can't send
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// A fresh set of ClientEnds
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	client := MakeClient(ends)

	cfg.mu.Lock()
	cfg.client = client
	cfg.mu.Unlock()

	svc := labrpc.MakeService(cfg.client)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(CLIENT, srv)
}

// Shut down an XPaxos client
func (cfg *config) crashClient(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // Disable client connections to the server

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	client := cfg.client
	if client != nil {
		cfg.mu.Unlock()
		client.Kill()
		cfg.mu.Lock()
		cfg.client = nil
	}
}

func (cfg *config) cleanup() {
	for i := 1; i < cfg.n; i++ {
		if cfg.xpServers[i] != nil {
			cfg.xpServers[i].Kill()
		}
	}
	if cfg.client != nil {
		cfg.client.Kill()
	}

	atomic.StoreInt32(&cfg.done, 1)
}

// Attach server i to the net.
func (cfg *config) connect(i int) {
	cfg.connected[i] = true

	// Outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// Incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// Detach server i from the net.
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// Outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// Incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) setUnreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) setLongReordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}


// Check that there's no leader
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, isLeader := cfg.xpServers[i].GetState()
			if isLeader {
				cfg.t.Fatalf("expected no leader, but %v claims to be leader\n", i)
			}
		}
	}
}

