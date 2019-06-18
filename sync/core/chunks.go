package core

import (
	"dbsync/sync/contract/strategy"
	"dbsync/sync/shared"
	"sync"
	"sync/atomic"
)

//Chunks represents chunksChan
type Chunks struct {
	strategy   *strategy.Chunk
	chunks     []*Chunk
	closed     uint32
	chunksChan chan *Chunk
	*sync.WaitGroup
	*sync.Mutex
}

//Range repeats handler call for each chunk
func (c *Chunks) Range(handler func(chunk *Chunk) error) error {
	for i := range c.chunks {
		chunk := c.chunks[i]
		if err := handler(chunk); err != nil {
			return err
		}
	}
	return nil
}

//Take returns a chunk from queue
func (c *Chunks) Take(ctx *shared.Context) *Chunk {
	select {
	case chunk := <-c.chunksChan:
		return chunk
	}
}

//CloseOffer closes chunk offering
func (c *Chunks) CloseOffer() {
	if ! atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return
	}
	for i := 0; i < c.strategy.Threads; i++ {
		c.chunksChan <- nil
	}
}

//Close closes chunks
func (c *Chunks) Close() {
	close(c.chunksChan)
}

//ChunkSize returns chunks size
func (c *Chunks) ChunkSize() int {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	result := len(c.chunks)
	return result
}

//Offer adds a chunks
func (c *Chunks) Offer(chunk *Chunk) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.chunks = append(c.chunks, chunk)
	c.chunksChan <- chunk
}

//NewChunks creates a new chunksChan
func NewChunks(strategy *strategy.Chunk) *Chunks {
	if strategy.Threads == 0 {
		strategy.Threads = 1
	}
	threads := strategy.Threads
	return &Chunks{
		strategy:   strategy,
		Mutex:      &sync.Mutex{},
		WaitGroup:  &sync.WaitGroup{},
		chunksChan: make(chan *Chunk, (2*threads)+1),
	}
}
