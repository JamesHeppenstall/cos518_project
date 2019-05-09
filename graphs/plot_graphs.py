import numpy as np
from matplotlib import pyplot as plt

def process_output_file(filename):
	latencies = []

	with open(filename, "r") as fp:
		for line in fp.readlines():
			if line.split()[-1] == "ns/op":
				latencies.append(int(line.split()[-2]))

	return latencies, len(latencies)

def calc_throughput_and_latency(latencies, op_size=1024, num_benchmarks=14):
	op_sizes = [op_size]

	for _ in range(num_benchmarks - 1):
		op_sizes.append(op_sizes[-1] * 2)

	op_sizes = np.array(op_sizes) / op_size
	latencies = np.array(latencies) / 1e6       # Measured in milliseconds (ms)
	throughput = np.array(op_sizes) / latencies # Measured in megabytes/second (MB/s)

	return throughput, latencies

def plot_throughput_vs_latency_1(n, xpaxos_latencies, paxos_latencies):
	xpaxos_throughput, xpaxos_latencies = calc_throughput_and_latency(xpaxos_latencies)
	paxos_throughput, paxos_latencies = calc_throughput_and_latency(paxos_latencies)

	plt.figure()
	plt.title("Throughput vs. Latency (No Faults)")
	plt.plot(xpaxos_throughput, xpaxos_latencies, "ro-", mfc="none", label="XPaxos (t={0})".format(n // 2))
	plt.plot(paxos_throughput, paxos_latencies, "go-", mfc="none", label="Paxos (t={0})".format(n // 2))
	plt.xlabel("Throughput (MB/s)")
	plt.ylabel("Latency (ms)")
	plt.legend()
	plt.show()

def plot_throughput_vs_latency_2(latencies_3, latencies_5, latencies_11):
	throughput_3, latencies_3 = calc_throughput_and_latency(latencies_3)
	throughput_5, latencies_5 = calc_throughput_and_latency(latencies_5)
	throughput_11, latencies_11 = calc_throughput_and_latency(latencies_11)

	plt.figure()
	plt.title("Throughput vs. Latency (No Faults)")
	plt.plot(throughput_3, latencies_3, "ro-", mfc="none", label="XPaxos (t=1)")
	plt.plot(throughput_5, latencies_5, "go-", mfc="none", label="XPaxos (t=2)")
	plt.plot(throughput_11, latencies_11, "bo-", mfc="none", label="XPaxos (t=5)")
	plt.xlabel("Throughput (MB/s)")
	plt.ylabel("Latency (ms)")
	plt.legend()
	plt.show()

def plot_time_vs_throughput(n, filename1, filename2, op_size=262144):
	latencies1, length = process_output_file(filename1)
	latencies2, _ = process_output_file(filename2)

	latencies = latencies1[:length // 2] + latencies2[:1] + latencies1[length // 2:]
	latencies = np.array(latencies) / 1e6
	throughput = (op_size / 1024) / latencies
	throughput[length // 2 - 1] = throughput[length // 2] # Correction that makes graph more readable

	plt.figure()
	plt.title("Time vs. Throughput (Single Crash Fault)")
	plt.plot(np.cumsum(latencies / 1e3), throughput, "b-", label="XPaxos (t={0})".format(n // 2))
	plt.xlabel("Time (s)")
	plt.ylabel("Throughput (MB/s)")
	plt.legend(loc=3)
	plt.show()

if __name__ == "__main__":
	 # Benchmark_3_0 - Number of XPaxos servers = 3 (t=1), No Faults, Network delta = 50ms
	xpaxos_latencies_3_0 = [1356030, 1384204, 1410490, 1445985, 1548104, 1740073, 2096868,
		2738180, 4230884, 7339803, 13293167, 24017686, 46432827, 100212926]

	paxos_latencies_3_0 = [726676, 753971, 769380, 804065, 850515, 959943, 1105449, 
		1282887, 1958755, 2912091, 4547516, 8552241, 16787261, 36280343]

	# Benchmark_5_0 - Number of XPaxos servers = 5 (t=2), No Faults, Network delta = 50ms
	xpaxos_latencies_5_0 = [1902003, 1973498, 2012234, 2068098, 2145884, 2372787, 2813421, 
		3826190, 6041209, 10682907, 19073795, 33974850, 65369896, 125670701]

	paxos_latencies_5_0 = [1411875, 1340863, 1360656, 1339810, 1469436, 1682367, 1958918, 
		2555338, 3420035, 5501513, 8208274, 15135304, 32572484, 70068151]

	# Benchmark_11_0 - Number of XPaxos servers = 11 (t=5), No Faults, Network delta = 50ms
	xpaxos_latencies_11_0 = [10214935, 10240691, 10465488, 10135348, 9853552, 10427422, 11377794, 
		13879637, 16800463, 24196789, 41005313, 67615353, 126686864, 241231328]

	paxos_latencies_11_0 = [3174589, 3095474, 3142733, 3283624, 3420335, 4018418, 4657095, 
		6219817, 8788313, 14276049, 20237393, 37946124, 78068078, 179761933]

	plot_throughput_vs_latency_1(3, xpaxos_latencies_3_0, paxos_latencies_3_0)
	plot_throughput_vs_latency_1(5, xpaxos_latencies_5_0, paxos_latencies_5_0)
	plot_throughput_vs_latency_1(11, xpaxos_latencies_11_0, paxos_latencies_11_0)
	plot_throughput_vs_latency_2(xpaxos_latencies_3_0, xpaxos_latencies_5_0, xpaxos_latencies_11_0)
	#plot_time_vs_throughput(3, "output_3_0_256kB.txt", "output_3_R_256kB.txt")
	#plot_time_vs_throughput(5, "output_5_0_256kB.txt", "output_5_R_256kB.txt")
	#plot_time_vs_throughput(11, "output_11_0_256kB.txt", "output_11_R_256kB.txt")
