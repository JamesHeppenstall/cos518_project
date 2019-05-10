package pbft

import (
	"fmt"
	"testing"
	"math/rand"
	"time"
)
func TestCommonCase3(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case - 1kB Operation (t=1)")

	op := make([]byte, 1024)
	rand.Read(op) // Operation is a 1 kB random byte array

	iters :=500
	for i := 0; i < iters; i++ {
		ok := cfg.client.Propose(op)
		for ok == false {
			time.Sleep(time.Duration(10) * time.Millisecond)
			ok = cfg.client.RePropose(op)
		}
	}

	fmt.Printf("Client Proposed: %d Client Committed: %d\n", cfg.client.timestamp - 1, cfg.client.committed)
	cfg.rpcCounts()
	cfg.checkLogs()
}

func (cfg *config) rpcCounts() {
	for i := 0; i < cfg.n; i++ {
		fmt.Printf("Server %d: RPC Count: %d\n", i, cfg.rpcCount(i))
	}
}

func (cfg *config) checkLogs() {
	for i := 1; i < cfg.n; i++ {
		for j := 1; j < cfg.pbftServers[i].executeSeqNum; j++ {
			fmt.Printf("Server %d Round %d Commits %d\n", i, j, len(cfg.pbftServers[i].commitLog[j].Msg1))
		}
	}
}