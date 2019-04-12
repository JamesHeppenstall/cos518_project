package xpaxos

import (
	"fmt"
	"labrpc"
	"sync"
	"time"
)

const CLIENT = 0

// A Go object implementing a single XPaxos peer
type Client struct {
	mu        sync.Mutex
	timestamp int
	// stats     stat_t
	replicas []*labrpc.ClientEnd
}

type ClientMsg struct {
	Operation interface{}
}

func (client *Client) Reply(args Args, reply *Reply) {
	fmt.Printf("Hello World\n")
}

func (client *Client) sendRequest(server int, args Args, reply *Reply) bool {
	ok := client.replicas[server].Call("XPaxos.PrintString", args, reply)
	return ok
}

func MakeClient(replicas []*labrpc.ClientEnd, applyCh chan ClientMsg) *Client {
	client := &Client{}

	client.mu.Lock()
	client.replicas = replicas
	client.mu.Unlock()


	go func() {
		args := Args{}
		args.Str = "Hello"

		reply := Reply{}
		if ok := client.sendRequest(1, args, &reply); ok {
			time.Sleep(1 * time.Second)
		}
	}()

	return client

}

func (xp *Client) Kill() {
}
