package criteria

import (
	"sync"
	"sync/atomic"
)

//Batch represents criteria batch
type Batch struct {
	mutex        *sync.RWMutex
	batchSize    int
	criteria     []map[string]interface{}
	values       map[string][]interface{}
	uniqueValues map[string]map[interface{}]bool
	counter      uint32
}

//Add add value to a batch
func (b *Batch) Add(values map[string]interface{}) {

	for key, value := range values {
		if b.hasValue(key, value) {
			continue
		}
		b.mutex.Lock()
		if _, ok := b.values[key]; !ok {
			b.values[key] = make([]interface{}, 0)
		}
		valueForKey := b.uniqueValues[key]
		if len(valueForKey) == 0 {
			valueForKey = make(map[interface{}]bool)
			b.uniqueValues[key] = valueForKey
		}
		valueForKey[key] = true
		b.values[key] = append(b.values[key], value)
		b.mutex.Unlock()
	}

	if int(atomic.AddUint32(&b.counter, 1)) >= b.batchSize {
		b.flush()
	}
}

func (b *Batch) flush() {
	if atomic.LoadUint32(&b.counter) == 0 {
		return
	}
	var criterion = make(map[string]interface{})
	for k, v := range b.values {
		criterion[k] = v
	}
	b.criteria = append(b.criteria, criterion)
	atomic.StoreUint32(&b.counter, 0)
	b.uniqueValues = make(map[string]map[interface{}]bool)
	b.values = make(map[string][]interface{})
}

func (b *Batch) hasValue(key string, value interface{}) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, ok := b.uniqueValues[key]; !ok {
		return false
	}
	valueForKey := b.uniqueValues[key]
	_, ok := valueForKey[value]
	return ok
}

//Get returns batched criteria
func (b *Batch) Get() []map[string]interface{} {
	b.flush()
	return b.criteria
}

//NewBatch creates a criteria batch
func NewBatch(batchSize int) *Batch {
	return &Batch{
		mutex:        &sync.RWMutex{},
		batchSize:    batchSize,
		criteria:     make([]map[string]interface{}, 0),
		values:       make(map[string][]interface{}),
		uniqueValues: make(map[string]map[interface{}]bool),
	}
}
