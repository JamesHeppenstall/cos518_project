package xpaxos

import (
	"fmt"
	"testing"
	"math/rand"
)

// TO RUN TESTS      - "go test -run=Test [-count=10]"
// TO RUN BENCHMARKS - "go test -run=Benchmark -bench=. [-benchtime=5s]" 
//
// => Try to set number of servers in each test to an even number (i.e. one client server
//    and an odd number of XPaxos servers)
// => The view change protocol can be slow when more than 3-4 servers fail so try to avoid
//    testing extreme scenarios (if you want to, be sure to set WAIT = true in common.go)

//
// ------------------------------ TEST FUNCTIONS ------------------------------
//
func TestCommonCase1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case - Null Operation (t=1)")

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

	fmt.Println("Test: Common Case - Null Operation (t>1)")

	iters := 5
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestCommonCase3(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case - 1kB Operation (t=1)")

	op := make([]byte, 1024)
	rand.Read(op) // Operation is a 1 kB random byte array

	iters := 1000
	for i := 0; i < iters; i++ {
		cfg.client.Propose(op)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
}

func TestCommonCase4(t *testing.T) {
	servers := 10
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case - 1kB Operation (t>1)")

	op := make([]byte, 1024)
	rand.Read(op) // Operation is a 1 kB random byte array

	iters := 1000
	for i := 0; i < iters; i++ {
		cfg.client.Propose(op)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
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
// Test sometimes fails if WAIT = true (because faulty server still finds its way into synchronous group)
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

//
// ---------------------------- BENCHMARK FUNCTIONS ---------------------------
//
func benchmark(n int, size int, b *testing.B) {
	servers := n // The number of XPaxos servers is n-1 (client included!) 
	cfg := makeConfig(nil, servers, false)
	defer cfg.cleanup()

	op := make([]byte, size)
	rand.Read(op) // Operation is random byte array of size bytes
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.client.Propose(op)
	}
}

func Benchmark_3_1kB(b *testing.B) { benchmark(4, 1024, b) }
func Benchmark_3_2kB(b *testing.B) { benchmark(4, 2048, b) }
func Benchmark_3_4kB(b *testing.B) { benchmark(4, 4096, b) }
func Benchmark_3_8kB(b *testing.B) { benchmark(4, 8192, b) }
func Benchmark_3_16kB(b *testing.B) { benchmark(4, 16384, b) }
func Benchmark_3_32kB(b *testing.B) { benchmark(4, 32768, b) }
func Benchmark_3_64kB(b *testing.B) { benchmark(4, 65536, b) }
func Benchmark_3_128kB(b *testing.B) { benchmark(4, 131072, b) }
func Benchmark_3_256kB(b *testing.B) { benchmark(4, 262144, b) }
func Benchmark_3_512kB(b *testing.B) { benchmark(4, 524288, b) }
func Benchmark_3_1MB(b *testing.B) { benchmark(4, 1048576, b) }
func Benchmark_3_2MB(b *testing.B) { benchmark(4, 2097152, b) }
func Benchmark_3_4MB(b *testing.B) { benchmark(4, 4194304, b) }
func Benchmark_3_8MB(b *testing.B) { benchmark(4, 8388608, b) }