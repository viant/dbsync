package core

import (
	"dbsync/sync/contract/strategy"
)

//Chunk represents a chunks
type Chunk struct {
	*strategy.Chunk
	Index    int
	Transferable
}


//NewChunk returns new chunk
func NewChunk(status *Status, filter map[string]interface{}) *Chunk{
	return &Chunk{Transferable: Transferable{Status:status, Filter:filter}}
}