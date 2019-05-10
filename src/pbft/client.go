package pbft

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
	iPrintf("Replicate: from client server (%d) to Pbft server (%d)\n", CLIENT, server)
	return client.replicas[server].Call("Pbft.Replicate", request, reply, CLIENT)
}

func (client *Client) issueReplicate(server int, request ClientRequest, replyCh chan bool) {
	reply := &Reply{}

	if ok := client.sendReplicate(server, request, reply); ok {
	}
}

func (client *Client) Propose(op interface{}) bool { // For simplicity, we assume the client's proposal is correct
	var timer <-chan time.Time
	client.timestamp++
	//client.mu.Lock()
	request := ClientRequest{
		MsgType:   REPLICATE,
		Timestamp: client.timestamp,
		Operation: op,
		ClientId:  CLIENT}

	replyCh := make(chan bool)
	client.vcCh = make(chan bool)

	rM := make(map[int]bool)
	client.replyMap = append(client.replyMap, rM)

	for server, _ := range client.replicas {
		if server != CLIENT {
			go client.issueReplicate(server, request, replyCh)
		}
	}

	if WAIT == false {
		timer = time.NewTimer(TIMEOUT * time.Millisecond).C
	}

	//client.mu.Unlock()

	select {
	case <-timer:
		iPrintf("Timeout: Client.Propose: client server (%d)\n", CLIENT)
		return false
	case <-replyCh:
		iPrintf("Success: committed request (%d)\n", client.timestamp)
		return true
	case <-client.vcCh:
		iPrintf("Success: committed request after view change (%d)", client.timestamp)
		return true
	}
}

func (client *Client) ConfirmVC(msg Message, reply *Reply) {
	client.vcCh <- true
}

func (client *Client) Reply(creply ClientReply, reply *Reply) {
	client.mu.Lock()
	defer client.mu.Unlock()
	iPrintf("%d %d %d %d", len(client.replyMap), creply.Timestamp, len(client.replyMap[creply.Timestamp]), creply.Commiter)
	client.replyMap[creply.Timestamp][creply.Commiter] = true
	if len(client.replyMap[creply.Timestamp]) >= 2*(len(client.replicas) - 2)/3 && client.committed < creply.Timestamp {
		client.committed = creply.Timestamp
		iPrintf("committed: %d", client.committed)
		client.vcCh <- true
	}
}

func (client *Client) RePropose(op interface{}) bool { // For simplicity, we assume the client's proposal is correct
	var timer <-chan time.Time
	iPrintf("Repropose")
	//client.mu.Lock()
	request := ClientRequest{
		MsgType:   REPLICATE,
		Timestamp: client.timestamp,
		Operation: op,
		ClientId:  CLIENT}

	replyCh := make(chan bool)
	client.vcCh = make(chan bool)

	//rM := make(map[int]bool)
	//client.replyMap = append(client.replyMap, rM)

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
		return false
	case <-replyCh:
		iPrintf("Success: committed request (%d)\n", client.timestamp)
		return true
	case <-client.vcCh:
		iPrintf("Success: committed request after view change (%d)", client.timestamp)
		return true
	}
}

//
// ------------------------------- MAKE FUNCTION ------------------------------
//
func MakeClient(replicas []*network.ClientEnd) *Client {
	client := &Client{}

	client.mu.Lock()
	client.replicas = replicas
	client.timestamp = 0
	client.committed = -1
	client.vcCh = make(chan bool)
	client.replyMap = make([]map[int]bool, 0)
	rM := make(map[int]bool)
	client.replyMap = append(client.replyMap, rM)

	client.mu.Unlock()

	return client
}

func (client *Client) Kill() {}

