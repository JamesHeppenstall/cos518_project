import numpy as np
from matplotlib import pyplot as plt

def calc_throughput_and_latency(latencies, op_size=1024, num_benchmarks=14):
	op_sizes = [op_size]

	for _ in range(num_benchmarks - 1):
		op_sizes.append(op_sizes[-1] * 2)

	op_sizes = np.array(op_sizes) / op_size
	latencies = np.array(latencies) / 1e6       # Measured in milliseconds (ms)
	throughput = np.array(op_sizes) / latencies # Measured in megabytes/second (MB/s)

	return throughput, latencies

def plot_throughput_vs_latency_1(n, latencies):
	throughput, latencies = calc_throughput_and_latency(latencies)

	plt.figure()
	plt.title("Throughput vs. Latency (Fault-Free)")
	plt.plot(throughput, latencies, "bo-", mfc="none", label="XPaxos (t={0})".format(n // 2))
	plt.xlabel("Throughput (MB/s)")
	plt.ylabel("Latency (ms)")
	plt.legend()
	plt.show()

def plot_throughput_vs_latency_2(latencies_3, latencies_5, latencies_11):
	throughput_3, latencies_3 = calc_throughput_and_latency(latencies_3)
	throughput_5, latencies_5 = calc_throughput_and_latency(latencies_5)
	throughput_11, latencies_11 = calc_throughput_and_latency(latencies_11)

	plt.figure()
	plt.title("Throughput vs. Latency (Fault-Free)")
	plt.plot(throughput_3, latencies_3, "ro-", mfc="none", label="XPaxos (t=1)")
	plt.plot(throughput_5, latencies_5, "go-", mfc="none", label="XPaxos (t=2)")
	plt.plot(throughput_11, latencies_11, "bo-", mfc="none", label="XPaxos (t=5)")
	plt.xlabel("Throughput (MB/s)")
	plt.ylabel("Latency (ms)")
	plt.legend()
	plt.show()

if __name__ == "__main__":
	 # Number of XPaxos servers = 3 (t=1)
	latencies_3 = [1370523, 1403328, 1414017, 1458120, 1548804, 1718874, 2079989,
		2764268, 4238630, 7200030, 13070145, 23446588, 45938973, 96908592]

	# Number of XPaxos servers = 5 (t=2)
	latencies_5 = [1880869, 1925783, 1954873, 2007609, 2147153, 2336045, 2810776, 
		3925262, 6001965, 10236870, 18271642, 35378452, 69094325, 124575934]

	# Number of XPaxos servers = 11 (t=5)
	latencies_11 = [10001447, 10013064, 10552734, 10844408, 11691880, 11553041, 10873228, 
		13479840, 18486121, 27493842, 37603711, 65881652, 120539304, 247515066]

	plot_throughput_vs_latency_1(3, latencies_3)
	plot_throughput_vs_latency_1(5, latencies_5)
	plot_throughput_vs_latency_1(11, latencies_11)
	plot_throughput_vs_latency_2(latencies_3, latencies_5, latencies_11)
