package main

import "xpaxos"

func main() {
	xp := &xpaxos.XPaxos{}
	print(xp.Id)

	print("Hello World")
}
