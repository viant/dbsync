package diff

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/criteria"
	"dbsync/sync/dao"

	"dbsync/sync/shared"
	"fmt"
)

type Service interface {
	Check(ctx *shared.Context, source, dest core.Record, filter map[string]interface{}) (*core.Status, error)

	UpdateStatus(ctx *shared.Context, status *core.Status, source, dest core.Record, filter map[string]interface{}, narrowInSyncSubset bool) (err error)

	Fetch(ctx *shared.Context, filter map[string]interface{}) (source, dest core.Record, err error)
}

//service finds source and dest difference status
type service struct {
	dao dao.Service
	*contract.Sync
	*core.Comparator
}

//Check checks source and dest difference status
func (d *service) Check(ctx *shared.Context, source, dest core.Record, filter map[string]interface{}) (*core.Status, error) {
	result := &core.Status{}
	err := d.UpdateStatus(ctx, result, source, dest, filter, true)
	return result, err
}

//Fetch reads source and dest signature records for supplied filter
func (d *service) Fetch(ctx *shared.Context, filter map[string]interface{}) (source, dest core.Record, err error) {
	if d.Diff.NewIDOnly && d.IDColumn() != "" {
		if dest, err = d.dao.Signature(ctx, contract.ResourceKindDest, filter); err == nil {
			destSignature := core.NewSignatureFromRecord(d.IDColumn(), dest)
			if len(filter) == 0 {
				filter = make(map[string]interface{})
			}
			filter[d.IDColumn()] = criteria.NewGraterThan(destSignature.Max())
		}
	}

	if source, err = d.dao.Signature(ctx, contract.ResourceKindSource, filter); err != nil {
		return nil, nil, err
	}
	dest, err = d.dao.Signature(ctx, contract.ResourceKindDest, filter)
	return source, dest, err
}

func (d *service) UpdateStatus(ctx *shared.Context, status *core.Status, source, dest core.Record, filter map[string]interface{}, narrowInSyncSubset bool) (err error) {
	defer func() {
		ctx.Log(fmt.Sprintf("(%v): in sync: %v\n", filter, status.InSync))
	}()
	status.InSync = d.Comparator.IsInSync(ctx, source, dest)
	if status.InSync {
		return nil
	}
	idColumn := d.Sync.IDColumn()
	hasID := idColumn != ""
	status.Source = core.NewSignatureFromRecord(idColumn, source)
	status.Dest = core.NewSignatureFromRecord(idColumn, dest)
	if len(dest) == 0 || status.Dest.Count() == 0 {
		status.Method = shared.SyncMethodInsert
		return nil
	}

	if ! hasID {
		status.Method = shared.SyncMethodDeleteInsert
		return nil
	}

	if status.Dest.Max() > status.Source.Max() || status.Dest.Count() > status.Source.Count() ||
		(status.Dest.Min() > 0 && status.Dest.Min() < status.Source.Min()) {
		status.Method = shared.SyncMethodDeleteMerge
		return nil
	}
	if !narrowInSyncSubset {
		return nil
	}

	narrowFilter := shared.CloneMap(filter)
	narrowFilter[idColumn] = criteria.NewLessOrEqual(status.Dest.Max())
	if d.isInSync(ctx, narrowFilter) {
		status.Method = shared.SyncMethodInsert
		status.SetInSyncWithID(status.Dest.Max())
		return nil
	}
	inSyncWithID := 0

	status.Method = shared.SyncMethodMerge
	idRange := core.NewIDRange(status.Source.Min(), status.Dest.Max())
	inSyncWithID, err = d.findMaxIDInSync(ctx, idRange, filter)
	if inSyncWithID > 0 {
		status.SetInSyncWithID(inSyncWithID)
	}
	return err
}

func (d *service) isInSync(ctx *shared.Context, filter map[string]interface{}) bool {
	candidate := &core.Status{}
	if source, dest, err := d.Fetch(ctx, filter); err == nil {
		if err = d.UpdateStatus(ctx, candidate, source, dest, filter, false); err == nil {
			return candidate.InSync
		}
	}
	return false
}

func (d *service) findMaxIDInSync(ctx *shared.Context, idRange *core.IDRange, filter map[string]interface{}) (int, error) {
	if len(filter) == 0 {
		filter = make(map[string]interface{})
	}
	inSyncDestMaxID := 0
	candidateID := idRange.Next(false)
	for i := 0; i < d.Diff.Depth; i++ {
		if idRange.Max <= 0 {
			break
		}
		filter[d.Sync.IDColumn()] = criteria.NewLessOrEqual(candidateID)
		inSync := d.isInSync(ctx, filter)
		if inSync {
			inSyncDestMaxID = candidateID
		}
		candidateID = idRange.Next(inSync)
	}
	return inSyncDestMaxID, nil
}

//NewService creates a new differ
func New(sync *contract.Sync, dao dao.Service) *service {
	return &service{Sync: sync, dao: dao, Comparator: core.NewComparator(&sync.Diff)}
}
