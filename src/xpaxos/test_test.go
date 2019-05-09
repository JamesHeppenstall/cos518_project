package xpaxos

import (
	"fmt"
	"math/rand"
	"testing"
)

// TO RUN TESTS      - "go test -run=Test [-count=10]"
// TO RUN BENCHMARKS - "go test -run=Benchmark -bench=."
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

	fmt.Println("Test: Full Network Partition - Single Crash Failure (t=1)")

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

	fmt.Println("Test: Full Network Partition - Single Crash Failure (t>1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestFullNetworkPartition3(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	cfg.net.SetFaultRate(2, 100)

	fmt.Println("Test: Full Network Partition - Multiple Crash Failures (t=1)")

	iters := 10
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)

	cfg.net.SetFaultRate(2, 0)
	cfg.net.SetFaultRate(3, 100)

	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)

	cfg.net.SetFaultRate(3, 0)
	cfg.net.SetFaultRate(1, 100)

	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
}

func TestFullNetworkPartition4(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	crash := rand.Intn(servers-1) + 1
	cfg.net.SetFaultRate(crash, 100)

	fmt.Println("Test: Full Network Partition - Multiple Crash Failures (t=1)")

	iters := 50
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		cfg.net.SetFaultRate(crash, 0)
		crash = rand.Intn(servers-1) + 1
		cfg.net.SetFaultRate(crash, 100)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
}

func TestFullNetworkPartition5(t *testing.T) {
	servers := 10
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	cfg.net.SetFaultRate(2, 100)
	cfg.net.SetFaultRate(4, 100)
	cfg.net.SetFaultRate(6, 100)

	fmt.Println("Test: Full Network Partition - Multiple Crash Failures (t>1)")

	iters := 10
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)

	cfg.net.SetFaultRate(2, 0)
	cfg.net.SetFaultRate(4, 0)
	cfg.net.SetFaultRate(6, 0)
	cfg.net.SetFaultRate(3, 100)
	cfg.net.SetFaultRate(5, 100)
	cfg.net.SetFaultRate(7, 100)

	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)

	cfg.net.SetFaultRate(3, 0)
	cfg.net.SetFaultRate(5, 0)
	cfg.net.SetFaultRate(7, 0)
	cfg.net.SetFaultRate(1, 100)
	cfg.net.SetFaultRate(8, 100)
	cfg.net.SetFaultRate(9, 100)

	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
}

func TestFullNetworkPartition6(t *testing.T) {
	servers := 10
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	crash1 := rand.Intn(servers-1) + 1
	crash2 := rand.Intn(servers-1) + 1
	cfg.net.SetFaultRate(crash1, 100)
	cfg.net.SetFaultRate(crash2, 100)

	fmt.Println("Test: Full Network Partition - Multiple Crash Failures (t>1)")

	iters := 20
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		cfg.net.SetFaultRate(crash1, 0)
		cfg.net.SetFaultRate(crash2, 0)
		crash1 = rand.Intn(servers-1) + 1
		crash2 = rand.Intn(servers-1) + 1
		cfg.net.SetFaultRate(crash1, 100)
		cfg.net.SetFaultRate(crash2, 100)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
}

func TestPartialNetworkPartition1(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	// XPaxos server (ID = 2) fails to send RPCs 50% of the time
	cfg.net.SetFaultRate(2, 50)

	fmt.Println("Test: Partial Network Partition - Single Partial Failure (t=1)")

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

	fmt.Println("Test: Partial Network Partition - Single Partial Failure (t>1)")

	iters := 3
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		comparePrepareSeqNums(cfg)
		compareExecuteSeqNums(cfg)
		comparePrepareLogEntries(cfg)
		compareCommitLogEntries(cfg)
	}
}

func TestPartialNetworkPartition3(t *testing.T) {
	servers := 4
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	partial := rand.Intn(servers-1) + 1
	cfg.net.SetFaultRate(partial, 50)

	fmt.Println("Test: Partial Network Partition - Multiple Partial Failures (t=1)")

	iters := 50
	for i := 0; i < iters; i++ {
		cfg.client.Propose(nil)
		cfg.net.SetFaultRate(partial, 0)
		partial = rand.Intn(servers-1) + 1
		cfg.net.SetFaultRate(partial, 50)
	}

	comparePrepareSeqNums(cfg)
	compareExecuteSeqNums(cfg)
	comparePrepareLogEntries(cfg)
	compareCommitLogEntries(cfg)
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

// Benchmark_3 - Number of XPaxos servers = 3 (t=1)
func Benchmark_3_1kB(b *testing.B)   { benchmark(4, 1024, b) }
func Benchmark_3_2kB(b *testing.B)   { benchmark(4, 2048, b) }
func Benchmark_3_4kB(b *testing.B)   { benchmark(4, 4096, b) }
func Benchmark_3_8kB(b *testing.B)   { benchmark(4, 8192, b) }
func Benchmark_3_16kB(b *testing.B)  { benchmark(4, 16384, b) }
func Benchmark_3_32kB(b *testing.B)  { benchmark(4, 32768, b) }
func Benchmark_3_64kB(b *testing.B)  { benchmark(4, 65536, b) }
func Benchmark_3_128kB(b *testing.B) { benchmark(4, 131072, b) }
func Benchmark_3_256kB(b *testing.B) { benchmark(4, 262144, b) }
func Benchmark_3_512kB(b *testing.B) { benchmark(4, 524288, b) }
func Benchmark_3_1MB(b *testing.B)   { benchmark(4, 1048576, b) }
func Benchmark_3_2MB(b *testing.B)   { benchmark(4, 2097152, b) }
func Benchmark_3_4MB(b *testing.B)   { benchmark(4, 4194304, b) }
func Benchmark_3_8MB(b *testing.B)   { benchmark(4, 8388608, b) }

// Benchmark_5 - Number of XPaxos servers = 5 (t=2)
func Benchmark_5_1kB(b *testing.B)   { benchmark(6, 1024, b) }
func Benchmark_5_2kB(b *testing.B)   { benchmark(6, 2048, b) }
func Benchmark_5_4kB(b *testing.B)   { benchmark(6, 4096, b) }
func Benchmark_5_8kB(b *testing.B)   { benchmark(6, 8192, b) }
func Benchmark_5_16kB(b *testing.B)  { benchmark(6, 16384, b) }
func Benchmark_5_32kB(b *testing.B)  { benchmark(6, 32768, b) }
func Benchmark_5_64kB(b *testing.B)  { benchmark(6, 65536, b) }
func Benchmark_5_128kB(b *testing.B) { benchmark(6, 131072, b) }
func Benchmark_5_256kB(b *testing.B) { benchmark(6, 262144, b) }
func Benchmark_5_512kB(b *testing.B) { benchmark(6, 524288, b) }
func Benchmark_5_1MB(b *testing.B)   { benchmark(6, 1048576, b) }
func Benchmark_5_2MB(b *testing.B)   { benchmark(6, 2097152, b) }
func Benchmark_5_4MB(b *testing.B)   { benchmark(6, 4194304, b) }
func Benchmark_5_8MB(b *testing.B)   { benchmark(6, 8388608, b) }

// Benchmark_11 - Number of XPaxos servers = 11 (t=5)
func Benchmark_11_1kB(b *testing.B)   { benchmark(12, 1024, b) }
func Benchmark_11_2kB(b *testing.B)   { benchmark(12, 2048, b) }
func Benchmark_11_4kB(b *testing.B)   { benchmark(12, 4096, b) }
func Benchmark_11_8kB(b *testing.B)   { benchmark(12, 8192, b) }
func Benchmark_11_16kB(b *testing.B)  { benchmark(12, 16384, b) }
func Benchmark_11_32kB(b *testing.B)  { benchmark(12, 32768, b) }
func Benchmark_11_64kB(b *testing.B)  { benchmark(12, 65536, b) }
func Benchmark_11_128kB(b *testing.B) { benchmark(12, 131072, b) }
func Benchmark_11_256kB(b *testing.B) { benchmark(12, 262144, b) }
func Benchmark_11_512kB(b *testing.B) { benchmark(12, 524288, b) }
func Benchmark_11_1MB(b *testing.B)   { benchmark(12, 1048576, b) }
func Benchmark_11_2MB(b *testing.B)   { benchmark(12, 2097152, b) }
func Benchmark_11_4MB(b *testing.B)   { benchmark(12, 4194304, b) }
func Benchmark_11_8MB(b *testing.B)   { benchmark(12, 8388608, b) }
