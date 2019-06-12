package data

import (
	"dbsync/sync/shared"
	"sync/atomic"
)

//Transferable represents transferable data set limited by filter
type Transferable struct {
	Suffix string
	Filter Record
	*Status
	Method      string
	IsDirect    bool
	OwnerSuffix string
	Transferred uint32
}

func (t *Transferable) SetTransferred(transferred int) {
	atomic.StoreUint32(&t.Transferred, uint32(transferred))
}



func (t Transferable) ShouldDelete() bool {
	return t.Method ==  shared.DMLDelete || t.Method==shared.SyncMethodDeleteInsert || t.Method == shared.SyncMethodDeleteMerge
}