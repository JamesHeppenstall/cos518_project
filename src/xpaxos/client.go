package xpaxos

// RPC handlers for an XPaxos client server (propose)
//
// client := MakeClient(replicas) - Creates an XPaxos client server
// => Option to perform cleanup with xp.Kill()

import (
	"network"
	"time"
)

//
// ---------------------------- REPLICATE/REPLY RPC ---------------------------
//
func (client *Client) sendReplicate(server int, request ClientRequest, reply *Reply) bool {
	dPrintf("Replicate: from client server (%d) to XPaxos server (%d)\n", CLIENT, server)
	return client.replicas[server].Call("XPaxos.Replicate", request, reply, CLIENT)
}

func (client *Client) issueReplicate(server int, request ClientRequest, replyCh chan bool) {
	reply := &Reply{}

	if ok := client.sendReplicate(server, request, reply); ok {
		if reply.Success == true { // Only the leader should reply to client server
			replyCh <- reply.Success
		}
	}
}

func (client *Client) Propose(op interface{}) { // For simplicity, we assume the client's proposal is correct
	var timer <-chan time.Time

	client.mu.Lock()
	defer client.mu.Unlock()

	request := ClientRequest{
		MsgType:   REPLICATE,
		Timestamp: client.timestamp,
		Operation: op,
		ClientId:  CLIENT}

	replyCh := make(chan bool)

	for server, _ := range client.replicas {
		if server != CLIENT {
			go client.issueReplicate(server, request, replyCh)
		}
	}

	if WAIT == false {
		timer = time.NewTimer(TIMEOUT * time.Millisecond).C
	}

	select {
	case <-timer:
		iPrintf("Timeout: Client.Propose: client server (%d)\n", CLIENT)
	case <-replyCh:
		iPrintf("Success: committed request (%d)\n", client.timestamp)
	case <-client.vcCh:
		iPrintf("Success: committed request after view change (%d)", client.timestamp)
	}

	client.timestamp++
}

func (client *Client) ConfirmVC(msg Message, reply *Reply) {
	client.vcCh <- true
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func MakeClient(replicas []*network.ClientEnd) *Client {
	client := &Client{}

	client.mu.Lock()
	client.replicas = replicas
	client.timestamp = 0
	client.vcCh = make(chan bool)
	client.mu.Unlock()

	return client
}

func (client *Client) Kill() {}
