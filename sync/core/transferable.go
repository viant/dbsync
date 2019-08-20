package core

import (
	"dbsync/sync/criteria"
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

//SetMinID sets min allowed ID filter
func (t *Transferable) SetMinID(key string, ID int) {
	if len(t.Filter) == 0 {
		t.Filter = make(map[string]interface{})
	}
	t.Filter[key] = criteria.NewGraterOrEqual(ID)
}

//SetTransferred set transferred value
func (t *Transferable) SetTransferred(transferred int) {
	atomic.StoreUint32(&t.Transferred, uint32(transferred))
}

//Clone clones this transferable
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

//Kind returns transferable kind
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

//ShouldDelete returns true if deletion is part of merging strategy
func (t Transferable) ShouldDelete() bool {
	if t.Status == nil {
		return false
	}
	return t.Method == shared.DMLDelete || t.Method == shared.SyncMethodDeleteInsert || t.Method == shared.SyncMethodDeleteMerge
}
