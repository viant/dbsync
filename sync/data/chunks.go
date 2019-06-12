package data

import (
	"dbsync/sync/model/strategy"
	"dbsync/sync/shared"
	"sync"
	"time"
)

//Chunks represents chunksChan
type Chunks struct {
	strategy   *strategy.Chunk
	chunks     []*Chunk
	chunksChan chan *Chunk
	closeChan  chan bool
	*sync.WaitGroup
	*sync.Mutex
}

//ChunkSize returns chunks size
func (c *Chunks) Range(handler func(chunk *Chunk) error) error {
	for i := range c.chunks {
		chunk := c.chunks[i]
		if err := handler(chunk); err != nil {
			return err
		}
	}
	return nil
}


func (c *Chunks) Take(ctx *shared.Context) *Chunk {
	select {
	case chunk := <-c.chunksChan:
		return chunk
	case <-c.closeChan:
		return nil

	}
}


func (c *Chunks) CloseOffer() {
	for i := 0; i < c.strategy.Threads; i++ {
		select {
		case c.closeChan <- true:
		case <-time.After(500 * time.Millisecond):
		}
	}
}


func (c *Chunks) Close() {
	close(c.closeChan)
	close(c.chunksChan)
}


//ChunkSize returns chunks size
func (c *Chunks) ChunkSize() int {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	result := len(c.chunks)
	return result
}

//Add adds a chunks
func (c *Chunks) Offer(chunk *Chunk) {
	c.chunksChan <- chunk
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.chunks = append(c.chunks, chunk)
}

//NewChunks creates a new chunksChan
func NewChunks(strategy *strategy.Chunk) *Chunks {
	if strategy.Threads == 0 {
		strategy.Threads = 1
	}
	threads := strategy.Threads
	return &Chunks{
		strategy:   strategy,
		Mutex:&sync.Mutex{},
		WaitGroup:  &sync.WaitGroup{},
		chunksChan: make(chan *Chunk, (2*threads)+1),
		closeChan:  make(chan bool, threads),
	}
}
