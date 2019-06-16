package criteria

import "sync"

//BatchMap represents a batch map
type BatchMap struct {
	batchSize int
	mutex     *sync.Mutex
	items     map[string]*Batch
}

//Range calls handler for each key, value pair
func (b *BatchMap) Range(handler func(key string, batch *Batch) error) (err error) {
	var items = make(map[string]*Batch)
	b.mutex.Lock()
	for k, v := range b.items {
		items[k] = v
	}
	b.mutex.Unlock()
	if len(items) == 0 {
		return nil
	}
	for k, v := range items {
		if err = handler(k, v); err != nil {
			break
		}
	}
	return err
}

//add adds key, value to the bathc
func (b *BatchMap) Add(key string, values map[string]interface{}) {
	b.mutex.Lock()
	batchMap, ok := b.items[key]
	if ! ok {
		b.items[key] = NewBatch(b.batchSize)
		batchMap = b.items[key]
	}
	b.mutex.Unlock()
	batchMap.Add(values)
}

//NewBatchSet creates a new batch set
func NewBatchMap(batchSize int) *BatchMap {
	return &BatchMap{
		batchSize: batchSize,
		mutex:     &sync.Mutex{},
		items:     make(map[string]*Batch),
	}
}
