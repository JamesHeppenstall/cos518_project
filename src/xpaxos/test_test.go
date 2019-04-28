package xpaxos

import (
	"fmt"
	"testing"
)

// => Try to set number of servers in each test to an even number (i.e. one client server
//    and an odd number of XPaxos servers)
// => The view change protocol can be slow when more than 3-4 servers fail so try to avoid
//    testing extreme scenarios (if you want to, be sure to set WAIT = true in common.go)

func TestCommonCase1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case (t=1)")

	iters := 5
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestCommonCase2(t *testing.T) {
	servers := 10
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case (t>1)")

	iters := 5
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestFullNetworkPartition1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	// XPaxos server (ID = 2) fails to send RPCs 100% of the time
	cfg.net.SetFaultRate(2, 100)

	fmt.Println("Test: Full Network Partition (t=1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}

	// It is very often (but not always) the case that the final view number is 6
	//if cfg.xpServers[1].view != 6 || cfg.xpServers[3].view != 6 {
	//	cfg.t.Fatal("Invalid current view (should be 6)!")
	//}
}

func TestFullNetworkPartition2(t *testing.T) {
	servers := 10
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	// XPaxos servers (ID = 2, 4, 6) fail to send RPCs 100% of the time
	cfg.net.SetFaultRate(2, 100)
	cfg.net.SetFaultRate(4, 100)
	cfg.net.SetFaultRate(6, 100)

	fmt.Println("Test: Full Network Partition (t>1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestPartialNetworkPartition1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	// XPaxos server (ID = 2) fails to send RPCs 50% of the time
	cfg.net.SetFaultRate(2, 50)

	fmt.Println("Test: Partial Network Partition (t=1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

// Test sometimes fails if WAIT = false (because client times out before view change protocol completes)
func TestPartialNetworkPartition2(t *testing.T) {
	servers := 10
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	// XPaxos servers (ID = 2, 4, 6) fail to send RPCs 75%, 50%, and 25% of the time
	cfg.net.SetFaultRate(2, 75)
	cfg.net.SetFaultRate(4, 50)
	cfg.net.SetFaultRate(6, 25)

	fmt.Println("Test: Partial Network Partition (t>1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}
