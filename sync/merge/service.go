package merge

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/shared"
	"dbsync/sync/sql"
	"fmt"
)

//Service represents a merge service
type Service interface {
	//Merge merges transferred transferable into dest table
	Merge(ctx *shared.Context, transferable *core.Transferable) error
	//Delete removes data from dest table for supplied filter, filter can not be emtpty or error
	Delete(ctx *shared.Context, filter map[string]interface{}) error
}

type service struct {
	Sync *contract.Sync
	dao  dao.Service
	*sql.Builder
	*shared.Mutex
}

func (s *service) delete(ctx *shared.Context, transferable *core.Transferable) error {
	deleteFilter := shared.CloneMap(transferable.Filter)
	DML, err := s.Builder.DML(shared.DMLDelete, transferable.Suffix, deleteFilter)
	if err != nil {
		return err
	}
	transferable.DML = DML
	return s.dao.ExecSQL(ctx, DML)
}

func (s *service) append(ctx *shared.Context, transferable *core.Transferable) error {
	DML := s.Builder.AppendDML(transferable.Suffix, transferable.OwnerSuffix)
	var err error
	transferable.DML = DML
	if err = s.dao.ExecSQL(ctx, DML); err == nil {
		err = s.dao.DropTransientTable(ctx, transferable.Suffix)
	}
	return err
}

//dedupeAppend removed all record from transient table that exist in dest, then appends only new
func (s *service) dedupeAppend(ctx *shared.Context, transferable *core.Transferable) (err error) {
	if len(s.Sync.IDColumns) == 0 {
		return s.append(ctx, transferable)
	}
	if transferable.OwnerSuffix == "" {
		DML, _ := s.Builder.DML(shared.TransientDMLDelete, transferable.Suffix, shared.CloneMap(transferable.Filter))
		transferable.DML = DML
		if err = s.dao.ExecSQL(ctx, DML); err != nil {
			return err
		}
	}
	return s.append(ctx, transferable)
}

func (s *service) merge(ctx *shared.Context, transferable *core.Transferable) error {
	DML, err := s.Builder.DML(s.MergeStyle, transferable.Suffix, shared.CloneMap(transferable.Filter))
	if err != nil {
		return err
	}
	transferable.DML = DML
	if err = s.dao.ExecSQL(ctx, DML); err == nil {
		err = s.dao.DropTransientTable(ctx, transferable.Suffix)
	}
	return err
}

//Delete delete data from dest table for supplied filter
func (s *service) Delete(ctx *shared.Context, filter map[string]interface{}) error {
	DML, _ := s.Builder.DML(shared.DMLFilteredDelete, "", filter)
	return s.dao.ExecSQL(ctx, DML)
}

//Merge merges data for supplied transferable
func (s *service) Merge(ctx *shared.Context, transferable *core.Transferable) (err error) {
	if transferable.IsDirect {
		return fmt.Errorf("transferable was direct")
	}
	if s.AppendOnly {
		if transferable.ShouldDelete() {
			if ctx.UseLock {
				s.Mutex.Lock(s.Sync.Table)
			}
			err = s.delete(ctx, transferable)
			if ctx.UseLock {
				s.Mutex.Unlock(s.Sync.Table)
			}
		}
		if err == nil {
			err = s.dedupeAppend(ctx, transferable)
		}
		return err
	}
	switch transferable.Method {
	case shared.SyncMethodDeleteInsert:
		if ctx.UseLock {
			s.Mutex.Lock(s.Sync.Table)
		}
		err = s.delete(ctx, transferable)
		if ctx.UseLock {
			s.Mutex.Unlock(s.Sync.Table)
		}
		if err == nil {
			err = s.dedupeAppend(ctx, transferable)
		}
		return err
	case shared.SyncMethodDeleteMerge:
		if ctx.UseLock {
			s.Mutex.Lock(s.Sync.Table)
		}
		if err = s.delete(ctx, transferable); err == nil {
			err = s.merge(ctx, transferable)
		}
		if ctx.UseLock {
			s.Mutex.Unlock(s.Sync.Table)
		}
		return err
	case shared.SyncMethodMerge:
		if ctx.UseLock {
			s.Mutex.Lock(s.Sync.Table)
		}
		err = s.merge(ctx, transferable)
		if ctx.UseLock {
			s.Mutex.Unlock(s.Sync.Table)
		}
		return err
	case shared.SyncMethodInsert:
		return s.dedupeAppend(ctx, transferable)
	}
	return fmt.Errorf("unknown transfer method: %v", transferable.Method)
}

//New creates a new
func New(sync *contract.Sync, dao dao.Service, mutex *shared.Mutex) Service {
	return &service{
		Sync:    sync,
		dao:     dao,
		Builder: dao.Builder(),
		Mutex:   mutex,
	}
}
