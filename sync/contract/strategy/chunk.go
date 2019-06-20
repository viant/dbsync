package strategy

import "dbsync/sync/shared"

const (
	defaultChunkThreads = 2
)

//ChunkStrategy represents chunk sync request part
type Chunk struct {
	Size     int `description:"chunk size in record count"`
	Threads  int
	SyncMode string `description:"persistency sync mode: batched or individual"`
	queued   uint32
}

//Init initializes chunk
func (c *Chunk) Init() error {
	if c.Size > 0 && c.Threads == 0 {
		c.Threads = defaultChunkThreads
	}
	if c.SyncMode == "" {
		c.SyncMode = shared.SyncModeBatch
	}
	return nil
}
