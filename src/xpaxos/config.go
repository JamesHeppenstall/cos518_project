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

	cfg.publishClientRPC()

	return cfg
}

func (cfg *config) publishClientRPC() {
	if (cfg.client != nil) {
		return
	}


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

	// Listen to messages from XPaxos indicating newly committed messages
	applyCh := make(chan ApplyMsg)

	xp := Make(ends, i, cfg.saved[i], applyCh)

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

	// Listen to messages from XPaxos indicating newly committed messages
	applyCh := make(chan ClientMsg)

	client := MakeClient(ends, applyCh)

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
	// fmt.Printf("connect(%d)\n", i)

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

// Check that there's exactly one leader
// Try a few times in case re-elections are needed
/*func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		leaders := make(map[int][]int)
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				if t, leader := cfg.rafts[i].GetState(); leader {
					leaders[t] = append(leaders[t], i)
				}
			}
		}

		lastTermWithLeader := -1
		for t, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders\n", t, len(leaders))
			}
			if t > lastTermWithLeader {
				lastTermWithLeader = t
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatal("expected one leader, got none")
	return -1
}*/

// check that everyone agrees on the term.
/*func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				cfg.t.Fatal("servers disagree on term")
			}
		}
	}
	return term
}*/

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

// How many servers think a log entry is committed?
/*func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	cmd := -1
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v\n",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}*/

// Wait for at least n servers to commit but don't wait forever.
/*func (cfg *config) wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range cfg.rafts {
				if t, _ := r.GetState(); t > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := cfg.nCommitted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d\n",
			nd, index, n)
	}
	return cmd
}*/

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
/*func (cfg *config) one(cmd int, expectedServers int) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		// try all the servers, maybe one is the leader.
		index := -1
		for si := 0; si < cfg.n; si++ {
			starts = (starts + 1) % cfg.n
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil {
				index1, _, ok := rf.Start(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd2, ok := cmd1.(int); ok && cmd2 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	cfg.t.Fatalf("one(%v) failed to reach agreement\n", cmd)
	return -1
}*/
