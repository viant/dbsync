package data

import (
	"dbsync/sync/criteria"
	"dbsync/sync/method"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)



//Partition represents a partition
type Partition struct {
	*method.Strategy
	IDColumn string
	Suffix string
	Values Record
	*Status
	*Chunks
	*sync.WaitGroup
	err error
	done int32
}

//IsDone returns true if partition sync is done
func (p *Partition) IsDone() bool {
	return atomic.LoadInt32(&p.done) == 1
}

//SetDone sets done
func (p *Partition) SetDone(done int32) {
	atomic.StoreInt32(&p.done, done)
}

//AddChunk add chunk
func (p *Partition) AddChunk(chunk *Chunk) {
	for k, v := range p.Values {
		if _, has := chunk.Criteria[k]; has {
			continue
		}
		chunk.Criteria[k] = v
	}
	chunk.Index = p.ChunkSize()
	chunk.Suffix = fmt.Sprintf("%v_chunk_%05d", p.Suffix, chunk.Index)

	chunk.Criteria[p.IDColumn] = criteria.NewBetween(chunk.Min(), chunk.Max())
	p.Chunks.Add(chunk)
}




//NewPartition returns new partition
func NewPartition(strategy *method.Strategy, record Record) *Partition {
	suffix := method.TransientTableSuffix
	columns := strategy.Partition.Columns
	if len(columns) > 0 {
		for _, column := range columns {
			value, ok := record.Value(column)
			if ! ok {
				continue
			}
			suffix += fmt.Sprintf("%v", value)
		}
	}
	IDColumn := ""
	if len(strategy.IDColumns) == 1 {
		IDColumn = strategy.IDColumns[0]
	}

	suffix = strings.Replace(suffix, "-", "", strings.Count(suffix, "-"))
	suffix = strings.Replace(suffix, "+", "", strings.Count(suffix, "+"))
	return &Partition{
		Strategy:  strategy,
		Suffix:    suffix,
		Values:    record,
		IDColumn:  IDColumn,
		WaitGroup: &sync.WaitGroup{},
		Chunks:    NewChunks(&strategy.Chunk),
	}
}
