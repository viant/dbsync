package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
	"sync"
	"sync/atomic"
)

//PartitionInfo represents partition info
type PartitionInfo struct {
	ProviderSQL string
	Columns     []string
	Threads     int
}

//BatchSize returns batch size for max elements
func (p *PartitionInfo) BatchSize(max int) int {
	batchSize := p.Threads
	if batchSize == 0 {
		batchSize = 1
	}
	if max < batchSize {
		batchSize = max
	}
	return batchSize
}

//Partition represents a partition
type Partition struct {
	PartitionInfo
	uniqueColumn   string
	criteriaValues map[string]interface{}
	Status         string
	SyncMethod     string
	SourceCount    int
	*Info
	err error
	*sync.WaitGroup
	Suffix string
	*Chunks
	done int32
}

//Partitions represents partitions
type Partitions struct {
	data      []*Partition
	index     map[string]*Partition
	keyColumn string
	channel   chan bool
	*sync.Mutex
	*sync.WaitGroup
}

//Range range over partition
func (p *Partitions) Range(handler func(partition *Partition) error) error {
	for _, partition := range p.data {
		p.Add(1)
		p.channel <- true
		go func(partition *Partition) {
			defer p.Done()
			partition.err = handler(partition)
			<-p.channel

		}(partition)
	}
	p.Wait()
	for _, partition := range p.data {
		if partition.err != nil {
			return partition.err
		}
	}
	return nil
}

//NewPartitions creates a new partitions
func NewPartitions(partitions []*Partition, threads int) *Partitions {
	var result = &Partitions{
		data:      partitions,
		channel:   make(chan bool, threads),
		Mutex:     &sync.Mutex{},
		index:     make(map[string]*Partition),
		WaitGroup: &sync.WaitGroup{},
	}
	if len(partitions) > 0 && len(partitions[0].criteriaValues) == 1 {
		for _, partition := range partitions {
			for k, value := range partition.criteriaValues {
				result.keyColumn = k
				result.index[toolbox.AsString(value)] = partition
			}
		}
	}

	return result
}

//CriteriaValues returns cloned criteria valiues
func (p *Partition) CriteriaValues() map[string]interface{} {
	var result = make(map[string]interface{})
	for k, v := range p.criteriaValues {
		result[k] = v
	}
	return result
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
	for k, v := range p.criteriaValues {
		if _, has := chunk.CriteriaValues[k]; has {
			continue
		}
		chunk.CriteriaValues[k] = v
	}
	chunk.Index = p.ChunkSize()
	chunk.Suffix = fmt.Sprintf("%v_chunk_%05d", p.Suffix, chunk.Index)

	chunk.CriteriaValues[p.uniqueColumn] = &between{from: chunk.Min(), to: chunk.Max()}
	p.Chunks.AddChunk(chunk)
}

//NewPartition returns new partition
func NewPartition(source PartitionInfo, values map[string]interface{}, chunkQueue int, uniqueColumn string) *Partition {
	suffix := transientTableSuffix
	if len(source.Columns) > 0 {
		for _, column := range source.Columns {
			suffix += fmt.Sprintf("%v", values[column])
		}
	}
	suffix = strings.Replace(suffix, "-", "", strings.Count(suffix, "-"))
	return &Partition{
		PartitionInfo:  source,
		Suffix:         suffix,
		criteriaValues: values,
		uniqueColumn:   uniqueColumn,
		WaitGroup:      &sync.WaitGroup{},
		Chunks:         NewChunks(chunkQueue),
	}
}
