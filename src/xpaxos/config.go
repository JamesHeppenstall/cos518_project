package xpaxos

import (
	crand "crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"network"
	"runtime"
	"sync/atomic"
	"testing"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeConfig(t *testing.T, n int, unreliable bool) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = network.MakeNetwork()
	cfg.n = n
	cfg.xpServers = make([]*XPaxos, cfg.n)
	cfg.client = &Client{}
	cfg.connected = make([]bool, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.privateKeys = make(map[int]*rsa.PrivateKey, cfg.n)
	cfg.publicKeys = make(map[int]*rsa.PublicKey, cfg.n)

	cfg.setUnreliable(unreliable)
	cfg.net.LongDelays(false)

	cfg.startClient() // Create client server

	for i := 1; i < cfg.n; i++ { // Create a full set of XPaxos servers
		cfg.start1(i)
	}

	for i := 0; i < cfg.n; i++ { // Connect everyone
		cfg.connect(i)
	}

	return cfg
}

// Shut down an XPaxos server
func (cfg *config) crash1(i int) {
	if i == CLIENT {
		dPrintf("Cannot call crash1() on client server; must call crashClient()")
		return
	}

	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // Disable client connections to the server

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	xp := cfg.xpServers[i]
	if xp != nil {
		cfg.mu.Unlock()
		xp.Kill()
		cfg.mu.Lock()
		cfg.xpServers[i] = nil
	}
}

// Start or re-start an XPaxos server; if one already exists, "kill" it first
// Allocate new outgoing port file names to isolate previous instance of this server since we 
// cannot really kill it
func (cfg *config) start1(i int) {
	if i == CLIENT {
		dPrintf("Cannot call start1() on client server; must call startClient()")
		return
	}

	cfg.crash1(i)

	// A fresh set of outgoing ClientEnd names so that old crashed instance's ClientEnds can't send
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// A fresh set of ClientEnds
	ends := make([]*network.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	// A fresh pair of RSA private/public keys
	privateKey, publicKey := generateKeys()
	cfg.privateKeys[i] = privateKey
	cfg.publicKeys[i] = publicKey

	xp := Make(ends, i, cfg.privateKeys[i], cfg.publicKeys)

	cfg.mu.Lock()
	cfg.xpServers[i] = xp
	cfg.mu.Unlock()

	svc := network.MakeService(xp)
	srv := network.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

// Shut down the client server
func (cfg *config) crashClient() {
	cfg.disconnect(CLIENT)
	cfg.net.DeleteServer(CLIENT) // Disable client connections to the server

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

// Start or re-start the client server; if one already exists, "kill" it first
// Allocate new outgoing port file names to isolate previous instance of this client since we cannot
// really kill it
func (cfg *config) startClient() {
	cfg.crashClient()

	// A fresh set of outgoing ClientEnd names so that old crashed instance's ClientEnds can't send
	cfg.endnames[CLIENT] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[CLIENT][j] = randstring(20)
	}

	// A fresh set of ClientEnds
	ends := make([]*network.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[CLIENT][j])
		cfg.net.Connect(cfg.endnames[CLIENT][j], j)
	}

	client := MakeClient(ends)

	cfg.mu.Lock()
	cfg.client = client
	cfg.mu.Unlock()

	svc := network.MakeService(cfg.client)
	srv := network.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(CLIENT, srv)
}

func (cfg *config) cleanup() {
	if cfg.client != nil {
		cfg.client.Kill()
	}

	for i := 1; i < cfg.n; i++ {
		if cfg.xpServers[i] != nil {
			cfg.xpServers[i].Kill()
		}
	}

	atomic.StoreInt32(&cfg.done, 1)
}

// Connect server i to the network
func (cfg *config) connect(i int) {
	if cfg.connected[i] == false {
		if i == CLIENT {
			dPrintf("Connected: client server (%d)\n", i)
		} else {
			dPrintf("Connected: XPaxos server (%d)\n", i)
		}
	}

	cfg.connected[i] = true

	for j := 0; j < cfg.n; j++ { // Outgoing ClientEnds
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	for j := 0; j < cfg.n; j++ { // Incoming ClientEnds
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// Disconnect server i from the network
func (cfg *config) disconnect(i int) {
	if cfg.connected[i] == true {
		if i == CLIENT {
			dPrintf("Disconnected: client server (%d)\n", i)
		} else {
			dPrintf("Disconnected: XPaxos server (%d)\n", i)
		}
	}

	cfg.connected[i] = false

	for j := 0; j < cfg.n; j++ { // Outgoing ClientEnds
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	for j := 0; j < cfg.n; j++ { // Incoming ClientEnds
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
