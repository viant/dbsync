package sync


const (
	ChunkStatusOk = "ok"
	ChunkStatusError = "error"
)

type Chunk struct {
	MinValue   *int
	MaxValue   *int
	CountValue *int
	Method string
	Status string
	Transfer *TransferJob
}

func (c *Chunk) Max() int {
	if c.MaxValue == nil {
		return 0
	}
	return *c.MaxValue
}

func (c *Chunk) Min() int {
	if c.MinValue == nil {
		return 0
	}
	return *c.MinValue
}


func (c *Chunk) Count() int {
	if c.CountValue == nil {
		return 0
	}
	return *c.CountValue
}