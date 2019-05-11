package pbft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestCommonCase3(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Common Case - 1kB Operation (t=1)")

	op := make([]byte, 1024)
	rand.Read(op) // Operation is a 1 kB random byte array

	iters := 500
	for i := 0; i < iters; i++ {
		ok := cfg.client.Propose(op)
		for ok == false {
			time.Sleep(time.Duration(10) * time.Millisecond)
			ok = cfg.client.RePropose(op)
		}
	}

	fmt.Printf("Client Proposed: %d Client Committed: %d\n", cfg.client.timestamp-1, cfg.client.committed)
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

func benchmarkNoFaults(n int, size int, b *testing.B) {
	servers := n // The number of PBFT servers is n-1 (client included!)
	cfg := makeConfig(nil, servers, false)
	defer cfg.cleanup()

	op := make([]byte, size)
	rand.Read(op) // Operation is random byte array of size bytes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.client.Propose(op)
	}
}

// Benchmark_3_0 - Number of PBFT servers = 4 (t=1), No Faults
func Benchmark_4_0_1kB(b *testing.B)   { benchmarkNoFaults(5, 1024, b) }
func Benchmark_4_0_2kB(b *testing.B)   { benchmarkNoFaults(5, 2048, b) }
func Benchmark_4_0_4kB(b *testing.B)   { benchmarkNoFaults(5, 4096, b) }
func Benchmark_4_0_8kB(b *testing.B)   { benchmarkNoFaults(5, 8192, b) }
func Benchmark_4_0_16kB(b *testing.B)  { benchmarkNoFaults(5, 16384, b) }
func Benchmark_4_0_32kB(b *testing.B)  { benchmarkNoFaults(5, 32768, b) }
func Benchmark_4_0_64kB(b *testing.B)  { benchmarkNoFaults(5, 65536, b) }
func Benchmark_4_0_128kB(b *testing.B) { benchmarkNoFaults(5, 131072, b) }
func Benchmark_4_0_256kB(b *testing.B) { benchmarkNoFaults(5, 262144, b) }
func Benchmark_4_0_512kB(b *testing.B) { benchmarkNoFaults(5, 524288, b) }
func Benchmark_4_0_1MB(b *testing.B)   { benchmarkNoFaults(5, 1048576, b) }
func Benchmark_4_0_2MB(b *testing.B)   { benchmarkNoFaults(5, 2097152, b) }
func Benchmark_4_0_4MB(b *testing.B)   { benchmarkNoFaults(5, 4194304, b) }
func Benchmark_4_0_8MB(b *testing.B)   { benchmarkNoFaults(5, 8388608, b) }

// Benchmark_5_0 - Number of PBFT servers = 7 (t=2), No Faults
func Benchmark_7_0_1kB(b *testing.B)   { benchmarkNoFaults(7, 1024, b) }
func Benchmark_7_0_2kB(b *testing.B)   { benchmarkNoFaults(7, 2048, b) }
func Benchmark_7_0_4kB(b *testing.B)   { benchmarkNoFaults(7, 4096, b) }
func Benchmark_7_0_8kB(b *testing.B)   { benchmarkNoFaults(7, 8192, b) }
func Benchmark_7_0_16kB(b *testing.B)  { benchmarkNoFaults(7, 16384, b) }
func Benchmark_7_0_32kB(b *testing.B)  { benchmarkNoFaults(7, 32768, b) }
func Benchmark_7_0_64kB(b *testing.B)  { benchmarkNoFaults(7, 65536, b) }
func Benchmark_7_0_128kB(b *testing.B) { benchmarkNoFaults(7, 131072, b) }
func Benchmark_7_0_256kB(b *testing.B) { benchmarkNoFaults(7, 262144, b) }
func Benchmark_7_0_512kB(b *testing.B) { benchmarkNoFaults(7, 524288, b) }
func Benchmark_7_0_1MB(b *testing.B)   { benchmarkNoFaults(7, 1048576, b) }
func Benchmark_7_0_2MB(b *testing.B)   { benchmarkNoFaults(7, 2097152, b) }
func Benchmark_7_0_4MB(b *testing.B)   { benchmarkNoFaults(7, 4194304, b) }
func Benchmark_7_0_8MB(b *testing.B)   { benchmarkNoFaults(7, 8388608, b) }

// Benchmark_11_0 - Number of PBFT servers = 16 (t=5), No Faults
func Benchmark_16_0_1kB(b *testing.B)   { benchmarkNoFaults(12, 1024, b) }
func Benchmark_16_0_2kB(b *testing.B)   { benchmarkNoFaults(12, 2048, b) }
func Benchmark_16_0_4kB(b *testing.B)   { benchmarkNoFaults(12, 4096, b) }
func Benchmark_16_0_8kB(b *testing.B)   { benchmarkNoFaults(12, 8192, b) }
func Benchmark_16_0_16kB(b *testing.B)  { benchmarkNoFaults(12, 16384, b) }
func Benchmark_16_0_32kB(b *testing.B)  { benchmarkNoFaults(12, 32768, b) }
func Benchmark_16_0_64kB(b *testing.B)  { benchmarkNoFaults(12, 65536, b) }
func Benchmark_16_0_128kB(b *testing.B) { benchmarkNoFaults(12, 131072, b) }
func Benchmark_16_0_256kB(b *testing.B) { benchmarkNoFaults(12, 262144, b) }
func Benchmark_16_0_512kB(b *testing.B) { benchmarkNoFaults(12, 524288, b) }
func Benchmark_16_0_1MB(b *testing.B)   { benchmarkNoFaults(12, 1048576, b) }
func Benchmark_16_0_2MB(b *testing.B)   { benchmarkNoFaults(12, 2097152, b) }
func Benchmark_16_0_4MB(b *testing.B)   { benchmarkNoFaults(12, 4194304, b) }
func Benchmark_16_0_8MB(b *testing.B)   { benchmarkNoFaults(12, 8388608, b) }
