package data

import (
	"dbsync/sync/method"
	"fmt"
	"sync"
)

//Partitions represents partitions
type Partitions struct {
	*method.Strategy
	values          []*Partition
	index           map[string]*Partition
	throttleChannel chan bool
	*sync.Mutex
	*sync.WaitGroup
}


//Get returns partition for supplied key
func (p *Partitions) Get(key string) (*Partition) {
	return p.index[key]
}

//Range range over partition
func (p *Partitions) Range(handler func(partition *Partition) error) error {
	partitions := p.values
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
	for _, partition := range p.values {
		if partition.err != nil {
			return partition.err
		}
	}
	return nil
}

//Validate checks if partition value for source and dest are valid
func (p *Partitions) Validate(comparator *Comparator, source, dest Record) error {
	keys := p.Partition.Columns
	if len(keys) == 0 {
		return nil
	}
	if ! comparator.AreKeysInSync(keys, source, dest) {
		return fmt.Errorf("inconsistent partition value: %v, src: %v, dest:%v", keys, source.Index(keys), dest.Index(keys))
	}
	return nil
}

//Key returns partition index key for the supplied record
func (p *Partitions) Key(record Record) string {
	return record.Index(p.Strategy.Partition.Columns)
}

//Init indexes partitions values
func (p *Partitions) Init() {
	partitionValues := p.values
	if len(p.values) == 0 || len(partitionValues[0].Values) == 0 {
		return
	}
	for _, partitionValue := range partitionValues {
		key := p.Key(partitionValue.Values)
		p.index[key] = partitionValue
	}
}


//NewPartitions creates a new partitions
func NewPartitions(values []*Partition, strategy *method.Strategy) *Partitions {
	threads := strategy.Partition.Threads
	if threads == 0 {
		threads = 1
	}
	return &Partitions{
		Strategy:        strategy,
		values:          values,
		throttleChannel: make(chan bool, threads),
		Mutex:           &sync.Mutex{},
		index:           make(map[string]*Partition),
		WaitGroup:       &sync.WaitGroup{},
	}
}
