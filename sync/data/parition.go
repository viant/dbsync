package data

import (
	"dbsync/sync/criteria"
	"dbsync/sync/method"
	"fmt"
	"github.com/viant/toolbox"
	"strings"
	"sync"
)


const partitionKeyTimeLayout = "20060102150405"


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


func (p *Partition) buildSuffix() string {
	suffix := method.TransientTableSuffix
	columns := p.Partition.Columns
	if len(columns) > 0 {
		for _, column := range columns {
			value, ok := p.Values.Value(column)
			if ! ok {
				continue
			}
			if toolbox.IsTime(value) {
				timeValue, _ := toolbox.ToTime(value, "")
				value = timeValue.Format(partitionKeyTimeLayout)
			}
			suffix += fmt.Sprintf("%v", value)
		}
	}
	suffix = strings.Replace(suffix, "-", "", strings.Count(suffix, "-"))
	suffix = strings.Replace(suffix, "+", "", strings.Count(suffix, "+"))
	return suffix
}


func (p *Partition) Init() {
	if len(p.IDColumns) == 1 {
		p.IDColumn = p.IDColumns[0]
	}
	p.Suffix = p.buildSuffix()
}


//NewPartition returns new partition
func NewPartition(strategy *method.Strategy, record Record) *Partition {
	return &Partition{
		Strategy:  strategy,
		Values:    record,
		WaitGroup: &sync.WaitGroup{},
		Chunks:    NewChunks(&strategy.Chunk),
	}
}
