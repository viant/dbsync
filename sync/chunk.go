package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"sync"
)

//ChunkInfo represents chunk info
type ChunkInfo struct {
	MinValue   interface{}
	MaxValue   interface{}
	CountValue interface{}
}

//Chunk represents a chunk
type Chunk struct {
	ChunkInfo
	Index          int
	Method         string
	Status         string
	Suffix         string
	CriteriaValues map[string]interface{}
	Transfer       *TransferJob
}

//Chunks represents chunks
type Chunks struct {
	chunks  []*Chunk
	channel chan bool
	mux     *sync.Mutex
}

//Max returns max chunk value
func (c *ChunkInfo) Max() int {
	if c.MaxValue == nil {
		return 0
	}
	return toolbox.AsInt(c.MaxValue)
}

//Min returns min chunk value
func (c *ChunkInfo) Min() int {
	if c.MinValue == nil {
		return 0
	}
	return toolbox.AsInt(c.MinValue)
}

//Count returns count chunk value
func (c *ChunkInfo) Count() int {
	if c.CountValue == nil {
		return 0
	}
	return toolbox.AsInt(c.CountValue)
}

//String returns chunk info
func (c *ChunkInfo) String() string {
	return fmt.Sprintf(`{"min":%v, "max":%v, "count":%v}`, c.Min(), c.Max(), c.Count())
}

//AddChunk adds a chunk
func (s *Chunks) AddChunk(chunk *Chunk) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.chunks = append(s.chunks, chunk)
}

//Chunk returns a chunk for an index
func (s *Chunks) Chunk(index int) *Chunk {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.chunks[index]
}

//ChunkSize returns chunk size
func (s *Chunks) ChunkSize() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	return len(s.chunks)
}

//NewChunks creates a new chunks
func NewChunks(queueSize int) *Chunks {
	return &Chunks{
		chunks:  make([]*Chunk, 0),
		channel: make(chan bool, queueSize),
		mux:     &sync.Mutex{},
	}
}
