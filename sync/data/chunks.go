package data

import (
	"dbsync/sync/method"
	"sync"
)

//Chunks represents chunks
type Chunks struct {
	strategy *method.Chunk
	chunks   []*Chunk
	channel  chan bool
	mux      *sync.Mutex
}

//ChunkSize returns chunk size
func (c *Chunks) ChunkSize() int {
	c.mux.Lock()
	defer c.mux.Unlock()
	return len(c.chunks)
}

//Add adds a chunk
func (c *Chunks) Add(chunk *Chunk) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.chunks = append(c.chunks, chunk)
}

//NewChunks creates a new chunks
func NewChunks(strategy *method.Chunk) *Chunks {
	return &Chunks{
		strategy: strategy,
		chunks:   make([]*Chunk, 0),
		channel:  make(chan bool, strategy.Size),
		mux:      &sync.Mutex{},
	}
}
