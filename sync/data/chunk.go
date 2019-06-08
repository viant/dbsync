package data

import (
	"dbsync/sync/method"
)

//Chunk represents a chunk
type Chunk struct {
	*method.Chunk
	Index    int
	Suffix   string
	Signature
	*Status
	Criteria map[string]interface{}
}

