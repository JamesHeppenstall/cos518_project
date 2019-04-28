package xpaxos

import (
	"fmt"
	"testing"
)

func TestCommonCase1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case (t=1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestCommonCase2(t *testing.T) {
	servers := 20
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case (t>1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestViewChange1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	cfg.net.SetFaultRate(2, 100)

	fmt.Println("Test: View Change (t=1)")

	iters := 3
	for i := 0; i < iters; i++ {
		fmt.Println(cfg.xpServers[1].view)
		fmt.Println(cfg.xpServers[2].view)
		fmt.Println(cfg.xpServers[3].view)
		cfg.client.Propose(nil)
		fmt.Println(cfg.xpServers[1].view)
		fmt.Println(cfg.xpServers[2].view)
		fmt.Println(cfg.xpServers[3].view)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func comparePrepareSeqNums(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		prepareSeqNum := cfg.xpServers[i].prepareSeqNum
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if i != j && cfg.xpServers[i].synchronousGroup[j] == true && cfg.xpServers[j].prepareSeqNum != prepareSeqNum {
					cfg.t.Fatal("Invalid prepare sequence numbers!")
				}
			}
		}
	}
}

func compareExecuteSeqNums(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		executeSeqNum := cfg.xpServers[i].executeSeqNum
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if i != j && cfg.xpServers[i].synchronousGroup[j] == true && cfg.xpServers[j].executeSeqNum != executeSeqNum {
					cfg.t.Fatal("Invalid execute sequence numbers!")
				}
			}
		}
	}
}

func comparePrepareLogEntries(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		prepareLogDigest := digest(cfg.xpServers[i].prepareLog)
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if cfg.xpServers[i].synchronousGroup[j] == true && digest(cfg.xpServers[j].prepareLog) != prepareLogDigest {
					cfg.t.Fatal("Invalid prepare logs!")
				}
			}
		}
	}
}

func compareCommitLogEntries(cfg *config) {
	currentView := getCurrentView(cfg)

	for i := 1; i < cfg.n; i++ {
		commitLogDigest := digest(cfg.xpServers[i].commitLog)
		if cfg.xpServers[i].view == currentView {
			for j := 1; j < cfg.n; j++ {
				if cfg.xpServers[i].synchronousGroup[j] == true && digest(cfg.xpServers[j].commitLog) != commitLogDigest {
					cfg.t.Fatal("Invalid commit logs!")
				}
			}
		}
	}
}

func getCurrentView(cfg *config) int {
	numCurrent := 0
	currentView := 0

	for _, xpServer := range cfg.xpServers[1:] {
		if xpServer.view > currentView {
			numCurrent = 1
			currentView = xpServer.view
		} else if xpServer.view == currentView {
			numCurrent++
		}
	}

	if numCurrent < (len(cfg.xpServers)+1)/2 {
		cfg.t.Fatal("Invalid current view!")
	}

	return currentView
}
