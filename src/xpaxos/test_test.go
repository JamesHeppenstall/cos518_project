package xpaxos

import (
	"testing"
	"fmt"
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

func comparePrepareSeqNums(cfg *config) {
	for i := 1; i < cfg.n; i++ {
		prepareSeqNum := cfg.xpServers[i].prepareSeqNum
		for j := 1; j < cfg.n; j++ {
			if i != j && cfg.xpServers[i].synchronousGroup[j] == true && cfg.xpServers[j].prepareSeqNum != prepareSeqNum {
				cfg.t.Fatal("Invalid prepare sequence numbers!")
			}
		}
	}
}

func compareExecuteSeqNums(cfg *config) {
	for i := 1; i < cfg.n; i++ {
		executeSeqNum := cfg.xpServers[i].executeSeqNum
		for j := 1; j < cfg.n; j++ {
			if i != j && cfg.xpServers[i].synchronousGroup[j] == true && cfg.xpServers[j].executeSeqNum != executeSeqNum {
				cfg.t.Fatal("Invalid execute sequence numbers!")
			}
		}
	}
}

func comparePrepareLogEntries(cfg *config) {
	for i := 1; i < cfg.n; i++ {
		prepareLogDigest := digest(cfg.xpServers[i].prepareLog)
		for j := 1; j < cfg.n; j++ {
			if cfg.xpServers[i].synchronousGroup[j] == true && digest(cfg.xpServers[j].prepareLog) != prepareLogDigest {
				cfg.t.Fatal("Invalid prepare logs!")
			}
		}
	}
}

func compareCommitLogEntries(cfg *config) {
	for i := 1; i < cfg.n; i++ {
		commitLogDigest := digest(cfg.xpServers[i].commitLog)
		for j := 1; j < cfg.n; j++ {
			if cfg.xpServers[i].synchronousGroup[j] == true && digest(cfg.xpServers[j].commitLog) != commitLogDigest {
				cfg.t.Fatal("Invalid commit logs!")
			}
		}
	}
}
