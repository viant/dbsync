package strategy

const defaultChunkThreads = 2


//ChunkStrategy represents chunk sync request part
type Chunk struct {
	SQL     string
	Size    int `description:"chunk size in record count"`
	Threads int
}


func (c *Chunk) Init() error {
	if c.Size > 0 && c.Threads == 0 {
		c.Threads = defaultChunkThreads
	}
	return nil
}
