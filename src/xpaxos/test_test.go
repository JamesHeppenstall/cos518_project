package xpaxos

import (
	"testing"
	//"fmt"
	//"time"
	//"math/rand"
	//"sync/atomic"
	//"sync"
)

func TestInitial(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	//fmt.Printf("Test: initial setup")

	cfg.client.Propose(nil)
	//cfg.client.Propose(nil)
	DPrintf("%d\n", len(cfg.xpServers[1].commitLog))
	DPrintf("%d\n", len(cfg.xpServers[2].commitLog))
	DPrintf("%d\n", len(cfg.xpServers[3].commitLog))
}
