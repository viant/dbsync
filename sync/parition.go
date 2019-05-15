package sync

import (
	"dbsync/sync/strategy"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

//Partition represents a partition
type Partition struct {
	strategy.Partition
	uniqueColumn string
	criteria     map[string]interface{}
	Status       string
	SourceCount  int
	DestCount    int
	*Info
	err error
	*sync.WaitGroup
	Suffix string
	*Chunks
	done int32
}

//Partitions represents partitions
type Partitions struct {
	data    []*Partition
	index   map[string]*Partition
	key     []string
	hasKey  bool
	channel chan bool
	*sync.Mutex
	*sync.WaitGroup
}

//Range range over partition
func (p *Partitions) Range(handler func(partition *Partition) error) error {
	partitions := p.data
	for _, partition := range partitions {
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

//Validate checks if partition value for source and dest are valid
func (p *Partitions) Validate(source, dest []Record) error {
	if len(source) != 1 || len(dest) != 1 || !p.hasKey {
		return nil
	}
	if !isMapItemsEqual(source[0], dest[0], p.key) {
		sourceIndex := keyValue(p.key, source[0])
		destIndex := keyValue(p.key, dest[0])
		return fmt.Errorf("inconsistent partition value: %v, src: %v, dest:%v", p.key, sourceIndex, destIndex)
	}
	return nil
}

//NewPartitions creates a new partitions
func NewPartitions(partitions []*Partition, session *Session) *Partitions {
	var result = &Partitions{
		data:      partitions,
		channel:   make(chan bool, session.Request.Partition.Threads),
		Mutex:     &sync.Mutex{},
		index:     make(map[string]*Partition),
		WaitGroup: &sync.WaitGroup{},
		key:       make([]string, 0),
	}
	if len(partitions) > 0 && len(partitions[0].criteria) > 0 {
		for key := range partitions[0].criteria {
			if session.Builder.isUpperCase {
				key = strings.ToUpper(key)
			}
			result.key = append(result.key, key)
		}
		sort.Strings(result.key)
		result.hasKey = len(result.key) > 0
		for _, partition := range partitions {
			result.index[keyValue(result.key, partition.criteria)] = partition
		}
	}
	return result
}

//CloneCriteria returns cloned filter values
func (p *Partition) CloneCriteria() map[string]interface{} {
	var result = make(map[string]interface{})
	for k, v := range p.criteria {
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
	for k, v := range p.criteria {
		if _, has := chunk.Criteria[k]; has {
			continue
		}
		chunk.Criteria[k] = v
	}
	chunk.Index = p.ChunkSize()
	chunk.Suffix = fmt.Sprintf("%v_chunk_%05d", p.Suffix, chunk.Index)

	chunk.Criteria[p.uniqueColumn] = &between{from: chunk.Min(), to: chunk.Max()}
	p.Chunks.AddChunk(chunk)
}

func (p *Partition) SetInfo(info *Info) {
	p.Info = info
	p.SourceCount = info.SourceCount
	p.DestCount = info.DestCount
}

//SetSynMethod sets sync method
func (p *Partition) SetSynMethod(method string) {
	if p.Method == SyncMethodMerge {
		return
	}
	p.Method = method
}

//NewPartition returns new partition
func NewPartition(source strategy.Partition, values map[string]interface{}, chunkQueue int, uniqueColumn string) *Partition {
	suffix := transientTableSuffix
	if len(source.Columns) > 0 {
		for _, column := range source.Columns {
			value := getValue(column, values)
			suffix += fmt.Sprintf("%v", value)
		}
	}
	suffix = strings.Replace(suffix, "-", "", strings.Count(suffix, "-"))
	suffix = strings.Replace(suffix, "+", "", strings.Count(suffix, "+"))
	return &Partition{
		Partition:    source,
		Suffix:       suffix,
		criteria:     values,
		uniqueColumn: uniqueColumn,
		WaitGroup:    &sync.WaitGroup{},
		Chunks:       NewChunks(chunkQueue),
	}
}
