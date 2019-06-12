package data

import (
	"sort"
	"sync"
)

//Transfers represents transfers
type Transfers struct {
	Transfers []*TransferJob
	mutex     *sync.RWMutex
}

//Add adds transfer job
func (t *Transfers) Add(job *TransferJob) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	sort.Sort(t)
	t.Transfers = append(t.Transfers, job)
}

//Len implements sort Len interface
func (t Transfers) Len() int { return len(t.Transfers) }

//Swap implements sort Swap interface
func (t Transfers) Swap(i, j int) { t.Transfers[i], t.Transfers[j] = t.Transfers[j], t.Transfers[i] }

//Less implements sort Less interface
func (t Transfers) Less(i, j int) bool {
	return t.Transfers[i].StartTime.Before(t.Transfers[j].StartTime)
}

//NewTransfers returns new transfers
func NewTransfers() *Transfers {
	return &Transfers{
		mutex:     &sync.RWMutex{},
		Transfers: make([]*TransferJob, 0),
	}
}

