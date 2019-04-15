package xpaxos

import (
	"labrpc"
	"sync"
	"time"
)

const TIMEOUT = 1000 // Timeout period (in milliseconds) for Propose()

type Client struct {
	mu        sync.Mutex
	replicas  []*labrpc.ClientEnd
	timestamp int
	// Must include statistics for evaluation
}

type ClientRequest struct {
	MsgType   int
	Timestamp int
	Operation interface{}
	ClientId  int
}

//
// ---------------------------- REPLICATE/REPLY RPC ---------------------------
//
type ReplicateReply struct {
	Success bool
}

func (client *Client) sendReplicate(server int, request ClientRequest, reply *ReplicateReply) bool {
	DPrintf("Replicate: from client server (%d) to XPaxos server (%d)\n", CLIENT, server)
	return client.replicas[server].Call("XPaxos.Replicate", request, reply)
}

func (client *Client) issueReplicate(server int, request ClientRequest, replyCh chan ReplicateReply) {
	reply := &ReplicateReply{}

	if ok := client.sendReplicate(server, request, reply); ok {
		if reply.Success == true { // Only the leader should reply to client server
			replyCh <- *reply
		}
	}
}

func (client *Client) Propose(op interface{}) {
	client.mu.Lock()
	defer client.mu.Unlock()

	request := ClientRequest{
		MsgType:   REPLICATE,
		Timestamp: client.timestamp,
		Operation: op,
		ClientId:  CLIENT}

	replyCh := make(chan ReplicateReply)

	for server, _ := range client.replicas {
		if server != CLIENT {
			go client.issueReplicate(server, request, replyCh)
		}
	}

	timer := time.NewTimer(TIMEOUT * time.Millisecond).C

	select {
	case <-timer:
		IPrintf("Timeout: client server (%d)\n", CLIENT)
	case <-replyCh:
		IPrintf("Success: committed request (%d)\n", client.timestamp)
	}

	client.timestamp++
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func MakeClient(replicas []*labrpc.ClientEnd) *Client {
	client := &Client{}

	client.mu.Lock()
	client.replicas = replicas
	client.timestamp = 0
	client.mu.Unlock()

	return client
}

func (client *Client) Kill() {}
