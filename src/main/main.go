package main

import (
	"time"
	"xpaxos"
)

func main() {
	servers := 2
	xpaxos.MakeConfig(nil, servers, false)
	time.Sleep(5 * time.Second)
	//print(cfg)
	//print("Hello World\n")
}
