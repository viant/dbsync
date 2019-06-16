package merge

import (
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/model"
	"dbsync/sync/shared"
	"dbsync/sync/sql"
	"fmt"
)

type Service interface {
	Merge(ctx *shared.Context, transferable *core.Transferable) error
}

type service struct {
	Sync *model.Sync
	dao  dao.Service
	*sql.Builder
	*shared.Mutex
}

func (s *service) delete(ctx *shared.Context, transferable *core.Transferable) error {
	DML, err := s.Builder.DML(shared.DMLDelete, transferable.Suffix, shared.CloneMap(transferable.Filter))
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

func (s *service) Merge(ctx *shared.Context, transferable *core.Transferable) (err error) {
	if transferable.IsDirect {
		return fmt.Errorf("transferable was direct")
	}
	s.Mutex.Lock(s.Sync.Table)
	defer s.Mutex.Unlock(s.Sync.Table)
	if s.AppendOnly {
		return s.dedupeAppend(ctx, transferable)
	}
	switch transferable.Method {
	case shared.SyncMethodDeleteInsert:
		if err = s.delete(ctx, transferable); err == nil {
			err = s.dedupeAppend(ctx, transferable)
		}
		return err
	case shared.SyncMethodDeleteMerge:
		if err = s.delete(ctx, transferable); err == nil {
			err = s.merge(ctx, transferable)
		}
		return err
	case shared.SyncMethodMerge:
		return s.merge(ctx, transferable)
	case shared.SyncMethodInsert:
		return s.dedupeAppend(ctx, transferable)
	}
	return fmt.Errorf("unknown transfer method: %v", transferable.Method)
}

//New creates a new
func New(sync *model.Sync, dao dao.Service, mutex *shared.Mutex) *service {
	return &service{
		Sync:    sync,
		dao:     dao,
		Builder: dao.Builder(),
		Mutex:   mutex,
	}
}
