package sync

import (
	"sync"
)

type indexedRecords struct {
	mux    *sync.Mutex
	source map[string][]Record
	dest   map[string][]Record
	key    []string
}

func (r *indexedRecords) build(records []Record, index map[string][]Record) {
	r.mux.Lock()
	defer r.mux.Unlock()
	for _, record := range records {
		keyValue := keyValue(r.key, record)
		if _, has := index[keyValue]; !has {
			index[keyValue] = make([]Record, 0)
		}
		index[keyValue] = append(index[keyValue], record)
	}

}

func newIndexedRecords(key []string) *indexedRecords {
	return &indexedRecords{
		mux:    &sync.Mutex{},
		source: make(map[string][]Record),
		dest:   make(map[string][]Record),
		key:    key,
	}
}
