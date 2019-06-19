package shared

import "sync"

//Mutex represents named mutex
type Mutex struct {
	*sync.Mutex
	dest map[string]*sync.Mutex
}

//Lock locks target name
func (m *Mutex) Lock(target string) {
	m.Mutex.Lock()
	mux, ok := m.dest[target]
	if !ok {
		m.dest[target] = &sync.Mutex{}
		mux = m.dest[target]
	}
	m.Mutex.Unlock()
	mux.Lock()
}

//Unlock unlocks target name
func (m *Mutex) Unlock(target string) {
	m.Mutex.Lock()
	mux, _ := m.dest[target]
	m.Mutex.Unlock()
	mux.Unlock()
}

//NewMutex create a new mutex
func NewMutex() *Mutex {
	return &Mutex{
		Mutex: &sync.Mutex{},
		dest:  make(map[string]*sync.Mutex),
	}
}
