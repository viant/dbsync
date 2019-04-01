package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"sync"
	"sync/atomic"
)

type Session struct {
	response      *SyncResponse
	Builder       *Builder
	Source        *Resource
	Dest          *Resource
	SourceDB      dsc.Manager
	DestDB        dsc.Manager
	Partitions    []map[string]interface{}
	err           error
	chunkChannel  chan bool
	syncWaitGroup *sync.WaitGroup
	chunks        []*Chunk
	closed        uint32
	mux           *sync.Mutex
}

func (s *Session) SetError(err error) bool {
	if err == nil || s.err != nil {
		return err != nil
	}
	fmt.Printf("SESSION ERROR %v !!!\n", err)
	s.err = err
	close(s.chunkChannel)
	atomic.StoreUint32(&s.closed, 1)
	s.response.SetError(err)
	return true
}

func (s *Session) AddChunk(chunk *Chunk) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.chunks = append(s.chunks, chunk)
}

func (s *Session) ChunkSize() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	return len(s.chunks)
}

func (s *Session) IsClosed() bool {
	return atomic.LoadUint32(&s.closed) == 1
}

func (s *Session) Chunk(index int) *Chunk {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.chunks[index]
}

func NewSession(request *SyncRequest, response *SyncResponse) (*Session, error) {
	builder, err := NewBuilder(request)
	if err != nil {
		return nil, err
	}
	var session = &Session{
		response:      response,
		Source:        request.Source,
		Dest:          request.Dest,
		Builder:       builder,
		syncWaitGroup: &sync.WaitGroup{},
		chunks:        make([]*Chunk, 0),
		mux:           &sync.Mutex{},
	}
	if session.SourceDB, err = dsc.NewManagerFactory().Create(request.Source.Config); err != nil {
		return nil, err
	}
	if session.DestDB, err = dsc.NewManagerFactory().Create(request.Dest.Config); err != nil {
		return nil, err
	}
	multiChunk := request.Sync.MultiChunk
	if multiChunk == 0 {
		multiChunk = 1
	}
	session.chunkChannel = make(chan bool, multiChunk)
	fmt.Printf("multi chunk %v\n", multiChunk)
	return session, nil
}
