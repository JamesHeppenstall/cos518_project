package network

import (
	"reflect"
	"sync"
)

const DEBUG = 1   // Debugging (0 = None, 1 = Info, 2 = Debug)
const DELTA = 100 // Network time frame delta for XPaxos synchronous group (in milliseconds)

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool                       // Pause a long time
	longReordering bool                       // Reorder replies by occaisionally delaying them
	ends           map[interface{}]*ClientEnd // Client endpoints by name
	enabled        map[interface{}]bool
	servers        map[interface{}]*Server     // Servers by name
	connections    map[interface{}]interface{} // Map of endpoint name to server name
	endCh          chan reqMsg
	faultRate      map[interface{}]int
}

type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int // Count of incoming RPCs
}

type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method
}

type ClientEnd struct {
	endname interface{} // Client endpoint's name
	ch      chan reqMsg // Copy of Network.endCh
}

type reqMsg struct {
	endname  interface{} // Name of sending client endpoint
	svcMeth  string      // i.e. "XPaxos.Replicate"
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
	callerId int
}

type replyMsg struct {
	ok    bool
	reply []byte
}
