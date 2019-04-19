package sync

import (
	"github.com/viant/toolbox"
	"sync"
)

type indexedRecords struct {
	mux    *sync.Mutex
	source map[string][]Record
	dest   map[string][]Record
	key    string
}

func (r *indexedRecords) build(records []Record, index map[string][]Record) {
	r.mux.Lock()
	defer r.mux.Unlock()
	for _, record := range records {

		value := toolbox.AsString(record[r.key])
		if _, has := index[value]; !has {
			index[value] = make([]Record, 0)
		}
		index[value] = append(index[value], record)
	}

}

func newIndexedRecords(key string) *indexedRecords {
	return &indexedRecords{
		mux:    &sync.Mutex{},
		source: make(map[string][]Record),
		dest:   make(map[string][]Record),
		key:    key,
	}
}
