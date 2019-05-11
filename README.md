# COS 518 Project - XPaxos Reimplementation

We reimplemented XPaxos in Golang for Princeton University's Advanced Computer Systems (COS 518) course. XPaxos is a cross fault-tolerant (XFT) consensus protocol for the state machine replication (SMR) problem. Specifically, we reimplemented the common case and view change protocols (without optimizations). For more information, consult the original paper at ```docs/XFT 1.pdf```. Tests and benchmarks can be run as follows:
```
go test -run=Test [-count=5]
go test -run=XXX -bench=. [-benchtime=100x]
```
For tests, set ```DEBUG = 1``` in ```src/xpaxos/common.go```. For benchmarks, set ```DEBUG = 0```. We evaluate XPaxos against Paxos, a crash fault-tolerant (CFT) protocol, and Practical Byzantine Fault Tolerance (PBFT), a byzantine fault-tolerant (BFT) protocol. Please note that our implementations of Paxos and PBFT are by no means complete and only used for evaluation purposes.
