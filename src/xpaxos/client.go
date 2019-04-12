package xpaxos

import (
	"fmt"
	"labrpc"
	"sync"
)

const CLIENT = 0

// A Go object implementing a single XPaxos peer
type Client struct {
	mu        sync.Mutex
	timestamp int
	// stats     stat_t
	replicas []*labrpc.ClientEnd
}

type ClientRequest struct {
	MsgType   int
	Timestamp int
	ClientId  int
}


func (client *Client) sendReplicate(server int, args ClientRequest, reply *ReplicateReply) bool {
	ok := client.replicas[server].Call("XPaxos.Replicate", args, reply)
	return ok
}

func (client *Client) propose() {
	msg := ClientRequest {
		MsgType:   REPLICATE,
		Timestamp: client.timestamp,
		ClientId:  0}

	reply := &ReplicateReply{}
	client.sendReplicate(1, msg, reply)

	client.timestamp++

	fmt.Printf("%d %d \n", reply.Msg1.ClientTimestamp, client.timestamp)
}


func MakeClient(replicas []*labrpc.ClientEnd) *Client {
	client := &Client{}

	client.mu.Lock()
	client.timestamp = 0
	client.replicas = replicas
	client.mu.Unlock()

	return client
}

func (xp *Client) Kill() {
}
