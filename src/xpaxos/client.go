package xpaxos

import (
	"labrpc"
	"time"
)

//
// ---------------------------- REPLICATE/REPLY RPC ---------------------------
//
func (client *Client) sendReplicate(server int, request ClientRequest, reply *Reply) bool {
	dPrintf("Replicate: from client server (%d) to XPaxos server (%d)\n", CLIENT, server)
	return client.replicas[server].Call("XPaxos.Replicate", request, reply, CLIENT)
}

func (client *Client) issueReplicate(server int, request ClientRequest, replyCh chan Reply) {
	reply := &Reply{}

	if ok := client.sendReplicate(server, request, reply); ok {
		if reply.Success == true { // Only the leader should reply to client server
			replyCh <- *reply
		} else if reply.IsLeader == true {
			go client.issueReplicate(server, request, replyCh)
		}
	}
}

func (client *Client) Propose(op interface{}) { // For simplicity, we assume the client's proposal is correct
	client.mu.Lock()
	defer client.mu.Unlock()

	request := ClientRequest{
		MsgType:   REPLICATE,
		Timestamp: client.timestamp,
		Operation: op,
		ClientId:  CLIENT}

	replyCh := make(chan Reply)

	for server, _ := range client.replicas {
		if server != CLIENT {
			go client.issueReplicate(server, request, replyCh)
		}
	}

	timer := time.NewTimer(TIMEOUT * time.Millisecond).C

	select {
	case <-timer:
		iPrintf("Timeout: Client.Propose: client server (%d)\n", CLIENT)
	case <-replyCh:
		iPrintf("Success: committed request (%d)\n", client.timestamp)
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
