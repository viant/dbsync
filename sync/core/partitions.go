package core

import (
	"dbsync/sync/contract/strategy"
	"dbsync/sync/criteria"
	"dbsync/sync/shared"
	"fmt"
	"github.com/viant/toolbox"
	"sync"
)

//Partitions represents partitions
type Partitions struct {
	*strategy.Strategy
	Source          []*Partition
	Dest            []*Partition
	CrossResource   bool
	index           map[string]*Partition
	throttleChannel chan bool
	*sync.Mutex
	*sync.WaitGroup
}

//Get returns partition for supplied key
func (p *Partitions) Get(key string) *Partition {
	return p.index[key]
}

//FindDateLayout finds date layout used in supplied record with one of the partition keys
func (p *Partitions) FindDateLayout(record map[string]interface{}) string {
	if len(p.Source) == 0 || len(record) == 0 {
		return ""
	}
	filter := p.Source[0].Filter
	if len(filter) == 0 {
		return ""
	}
	for k, v := range record {
		if val, ok := filter[k]; ok {
			if toolbox.IsTime(v) {
				return p.Strategy.Diff.DateLayout[0:len(toolbox.AsString(val))]
			}
		}
	}
	return ""
}

//BatchTransferable returns batched transferable
func (p *Partitions) BatchTransferable() *Transferable {
	result := &Transferable{
		Suffix: shared.TransientTableSuffix,
		Status: &Status{
			Method: shared.SyncMethodInsert,
			Source: &Signature{},
			Dest:   &Signature{},
		},
	}

	partitions := p.Source
	for i := 0; i < len(partitions); i++ {
		transferable := partitions[i].Transferable
		if transferable.ShouldDelete() {
			continue
		}
		if transferable.Status == nil {
			continue
		}
		if transferable.Method != shared.SyncMethodInsert {
			result.Method = transferable.Method
		}
		result.Source.CountValue = result.Source.Count() + transferable.Source.Count()
	}
	return result
}

//Criteria returns partitions criteria
func (p *Partitions) Criteria() []map[string]interface{} {
	batch := criteria.NewBatch(p.Strategy.Diff.BatchSize)
	_ = p.Range(func(partition *Partition) error {
		batch.Add(partition.Filter)
		return nil
	})
	result := batch.Get()
	if len(result) == 0 {
		result = append(result, map[string]interface{}{})
	}
	return result
}

//Range range over partition
func (p *Partitions) Range(handler func(partition *Partition) error) error {
	partitions := p.Source
	for i := range partitions {
		p.Add(1)
		p.throttleChannel <- true
		go func(partition *Partition) {
			defer p.Done()
			partition.err = handler(partition)
			<-p.throttleChannel
		}(partitions[i])
	}

	p.Wait()
	for _, partition := range p.Source {
		if partition.err != nil {
			return partition.err
		}
	}
	return nil
}

//Validate checks if partition value for Source and dest are valid
func (p *Partitions) Validate(ctx *shared.Context, comparator *Comparator, source, dest Record) error {
	keys := p.Partition.Columns
	if len(keys) == 0 {
		return nil
	}
	if len(dest) == 0 {
		return nil
	}
	if !comparator.AreKeysInSync(ctx, keys, source, dest) {

		return fmt.Errorf("inconsistent partition value: %v, src: %v, dest:%v", keys, source.Index(keys), dest.Index(keys))
	}
	return nil
}

//Key returns partition Index key for the supplied record
func (p *Partitions) Key(record Record) string {
	return record.Index(p.Strategy.Partition.Columns)
}

//Keys returns all partition keys
func (p *Partitions) Keys() []string {
	var result = make([]string, 0)
	for k := range p.index {
		result = append(result, k)
	}
	return result
}

//Init indexes partitions Source
func (p *Partitions) Init() {
	items := p.Source
	if len(p.Source) == 0 || len(items[0].Filter) == 0 {
		return
	}
	for i := range items {
		partition := items[i]
		partition.Init()
		key := p.Key(partition.Filter)
		p.index[key] = partition
	}
}

//NewPartitions creates a new partitions
func NewPartitions(source []*Partition, strategy *strategy.Strategy) *Partitions {
	if len(source) == 0 {
		partition := NewPartition(strategy, Record{})
		source = []*Partition{partition}
	}
	threads := strategy.Partition.Threads
	if threads == 0 {
		strategy.Partition.Threads = 1
		threads = 1
	}
	return &Partitions{
		Strategy:        strategy,
		Source:          source,
		throttleChannel: make(chan bool, threads),
		Mutex:           &sync.Mutex{},
		index:           make(map[string]*Partition),
		WaitGroup:       &sync.WaitGroup{},
	}
}
