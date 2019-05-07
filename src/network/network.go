package network

// Channel-based RPC network adapted from Golang's net/rpc/server.go
// Sends gob-encoded values to ensure that RPCs don't include references to program objects
//
// Originally written for MIT's 6.824 (Distributed Systems) course and modified for use as
// a controlled network environment in our Princeton COS 518 (Advanced Computer Systems)
// final project because XPaxos assumes a *strong* network model
//
// net := MakeNetwork()              - Holds network, clients, servers
// end := net.MakeEnd(endname)       - Create a client endpoint to talk to one server
// net.AddServer(servername, server) - Add a named server to network
// net.DeleteServer(servername)      - Eliminate a named server from network
// net.Connect(endname, servername)  - Connect a client to a server
// net.Enable(endname, enabled)      - Enable/disable a client
// net.Reliable(bool)                - False means drop/delay messages
//
// end.Call("XPaxos.Replicate", args, &reply) - Send an RPC and wait for reply
// => "XPaxos" is the name of the server struct to be called
// => "Replicate" is the name of the method to be called
// => Call() returns true to indicate that the server executed the request and the reply
//    is valid (no cryptographic verification though!)
// => Call() returns false if the network lost the request/reply or the server is down
// => It's OK to have multiple Call()'s in progress at the same time on the same ClientEnd
// => Concurrent calls to Call() may be delivered to the server out of order since the network
//    may reorder messages
// => Call() is guaranteed to return (perhaps after a delay) *except* if the handler function
//    on the server-side does not return
// => The server RPC handler function must declare its reply arguments as pointers, so that
//    their types exactly match the types of the arguments to Call()
//
// srv := MakeServer() - Holds a collection of services all sharing the same RPC dispatcher
// srv.AddService(svc) - A server can have multiple services (i.e. XPaxos and k/v)
// => Pass srv to net.AddServer()
//
// svc := MakeService(receiverObject) - Object's methods that will handle RPCs
// => Very much like Golang's rpcs.Register()
// => Pass svc to srv.AddService()

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"time"
)

//
// ------------------------------- CALL FUNCTION ------------------------------
//
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}, callerId int) bool {
	// The return value indicates success; false means the server couldn't be contacted
	req := reqMsg{}
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)
	req.callerId = callerId

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	e.ch <- req

	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

//
// ----------------------------- NETWORK FUNCTIONS ----------------------------
//
func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	rn.endCh = make(chan reqMsg)
	rn.faultRate = map[interface{}]int{}

	go func() { // Single goroutine to handle all ClientEnd.Call()'s
		for xreq := range rn.endCh {
			go rn.ProcessReq(xreq)
		}
	}()

	return rn
}

func (rn *Network) SetFaultRate(server int, rate int) {
	rn.faultRate[server] = rate
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

func (rn *Network) ReadEndnameInfo(endname interface{}) (enabled bool, servername interface{},
	server *Server, reliable bool, longreordering bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

func (rn *Network) IsServerDead(endname interface{}, servername interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.enabled[endname] == false || rn.servers[servername] != server {
		return true
	}
	return false
}

func (rn *Network) ProcessReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.ReadEndnameInfo(req.endname)

	if enabled && servername != nil && server != nil {
		if reliable == false {
			ms := (rand.Int() % 27) // Artifically create a short random delay
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil} // Drop the request and return as if timeout
			return
		}

		if (rand.Int() % 100) < rn.faultRate[servername] { // Failure when sending to destination
			dPrintf("Network: couldn't connect XPaxos server (%d) to XPaxos server (%d)\n", req.callerId, servername)
			time.Sleep(time.Duration(DELTA) * time.Millisecond)
			req.replyCh <- replyMsg{false, nil} // Drop the request and return as if timeout
			return
		}

		// Execute the request in a separate thread so that we can periodically check if the server
		// has been killed and the RPC should get a failure reply
		ech := make(chan replyMsg)
		go func() {
			r := server.dispatch(req)
			ech <- r
		}()

		// Wait for RPC handler to return but stop waiting if DeleteServer() has been called and
		// return an error.
		var reply replyMsg
		replyOK := false
		serverDead := false
		for replyOK == false && serverDead == false {
			select {
			case reply = <-ech:
				if (rand.Int() % 100) < rn.faultRate[req.callerId] { // Failure when sending to source
					dPrintf("Network: couldn't connect XPaxos server (%d) to XPaxos server (%d)\n", servername, req.callerId)
					time.Sleep(time.Duration(DELTA) * time.Millisecond)
					req.replyCh <- replyMsg{false, nil} // Drop the request and return as if timeout
					return
				}
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = rn.IsServerDead(req.endname, servername, server)
			}
		}

		// Do not reply if DeleteServer() has been called to avoid situation in which a client gets a
		// positive reply but the server persisted the update
		serverDead = rn.IsServerDead(req.endname, servername, server)

		if replyOK == false || serverDead == true {
			req.replyCh <- replyMsg{false, nil} // Server was killed while we were waiting; return error
		} else if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil} // Drop the reply and return as if timeout
		} else if longreordering == true && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000)) // Artificially delay the response for a while
			time.Sleep(time.Duration(ms) * time.Millisecond)
			req.replyCh <- reply
		} else {
			req.replyCh <- reply
		}
	} else { // Simulate no reply and an eventual timeout
		ms := 0
		if rn.longDelays {
			ms = (rand.Int() % 7000)
		} else {
			ms = (rand.Int() % 100)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		req.replyCh <- replyMsg{false, nil}
	}
}

func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	e := &ClientEnd{}
	e.endname = endname
	e.ch = rn.endCh
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	return e
}

func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

func (rn *Network) DeleteServer(servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = nil
}

// Note that a ClientEnd can only be connected once in its lifetime
func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.connections[endname] = servername
}

func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.enabled[endname] = enabled
}

// Get a server's count of incoming RPCs
func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

//
// ----------------------------- SERVER FUNCTIONS -----------------------------
//
func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()
	rs.count += 1

	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]

	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k, _ := range rs.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

//
// ----------------------------- SERVICE FUNCTIONS ----------------------------
//
func MakeService(rcvr interface{}) *Service { // A single server may have more than one Service
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		//fmt.Printf("%v pp %v ni %v 1k %v 2k %v no %v\n",
		//	mname, method.PkgPath, mtype.NumIn(), mtype.In(1).Kind(), mtype.In(2).Kind(), mtype.NumOut())

		// The method is not suitable for an RPC handler
		if method.PkgPath != "" || mtype.NumIn() != 3 || mtype.In(2).Kind() != reflect.Ptr || mtype.NumOut() != 0 {
			//fmt.Printf("bad method: %v\n", mname)
		} else {
			svc.methods[mname] = method
		}
	}

	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok { // Prepare space into which to read the argument
		args := reflect.New(req.argsType) // The value's type will be a pointer to req.argsType

		// (1) Decode the argument
		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface())

		// (2) Allocate space for the reply
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		// (3) Call the method
		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		// (4) Encode the reply
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}
