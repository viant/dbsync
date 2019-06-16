package core

import (
	"dbsync/sync/shared"
	"sync/atomic"
)

//Transferable represents transferable data set limited by filter
type Transferable struct {
	OwnerSuffix string
	Suffix      string
	Filter      Record
	*Status
	IsDirect    bool
	Transferred uint32
	ID          string
	DQL         string
	DML         string
}

func (t *Transferable) SetTransferred(transferred int) {
	atomic.StoreUint32(&t.Transferred, uint32(transferred))
}

func (t Transferable) Clone() *Transferable {
	return &Transferable{
		OwnerSuffix: t.OwnerSuffix,
		Suffix:      t.Suffix,
		Filter:      t.Filter,
		Status:      t.Status.Clone(),
		IsDirect:    t.IsDirect,
		Transferred: t.Transferred,
		ID:          t.ID,
		DQL:         t.DQL,
		DML:         t.DML,
	}
}

func (t Transferable) Kind() string {
	if t.IsDirect {
		return shared.SyncKindDirect
	}
	switch t.Method {
	case "":
		return shared.SyncKindInSync
	default:
		return t.Method
	}
}

func (t Transferable) ShouldDelete() bool {
	return t.Method == shared.DMLDelete || t.Method == shared.SyncMethodDeleteInsert || t.Method == shared.SyncMethodDeleteMerge
}
