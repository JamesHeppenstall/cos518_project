package xpaxos

import "sync"

type Persister struct {
	mu          sync.Mutex
	xPaxosState []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.xPaxosState = ps.xPaxosState
	return np
}

func (ps *Persister) SaveXPaxosState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.xPaxosState = data
}

func (ps *Persister) ReadXPaxosState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.xPaxosState
}

func (ps *Persister) XPaxosStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.xPaxosState)
}
