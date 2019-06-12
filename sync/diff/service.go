package diff

import (
	"dbsync/sync/criteria"
	"dbsync/sync/dao"
	"dbsync/sync/data"
	"dbsync/sync/model"
	"dbsync/sync/shared"
	"fmt"
)

type Service interface {
	
	Check(ctx *shared.Context, source, dest data.Record, filter map[string]interface{}) (*data.Status, error)

	UpdateStatus(ctx *shared.Context, status *data.Status, source, dest data.Record, filter map[string]interface{}, narrowInSyncSubset bool) (err error)

	Fetch(ctx *shared.Context, filter map[string]interface{}) (source, dest data.Record, err error)
}

//service finds source and dest difference status
type service struct {
	dao dao.Service
	*model.Sync
	*data.Comparator
}

//Check checks source and dest difference status
func (d *service) Check(ctx *shared.Context, source, dest data.Record, filter map[string]interface{}) (*data.Status, error) {
	result := &data.Status{}
	err := d.UpdateStatus(ctx, result, source, dest, filter, true)
	return result, err
}

//Fetch reads source and dest signature records for supplied filter
func (d *service) Fetch(ctx *shared.Context, filter map[string]interface{}) (source, dest data.Record, err error) {
	if source, err = d.dao.Signature(ctx, model.ResourceKindSource, filter); err != nil {
		return nil, nil, err
	}
	dest, err = d.dao.Signature(ctx, model.ResourceKindDest, filter)
	return source, dest, err
}

func (d *service) UpdateStatus(ctx *shared.Context, status *data.Status, source, dest data.Record, filter map[string]interface{}, narrowInSyncSubset bool) (err error) {
	defer func() {
		ctx.Log(fmt.Sprintf("(%v): in sync: %v\n", filter, status.InSync))
	}()
	if status.InSync = d.Comparator.IsInSync(ctx, source, dest); status.InSync {
		return nil
	}
	idColumn := d.Sync.IDColumn()
	hasID := idColumn != ""
	status.Source = data.NewSignatureFromRecord(idColumn, source)
	status.Dest = data.NewSignatureFromRecord(idColumn, dest)
	if len(dest) == 0 || status.Dest.Count() == 0 {
		status.Method = shared.SyncMethodInsert
		return nil
	}

	if ! hasID {
		status.Method = shared.SyncMethodDeleteInsert
		return nil
	}
	if status.Dest.Max() > status.Dest.Max() || status.Dest.Count() > status.Source.Count() || (status.Dest.Min() > 0 && status.Dest.Min() < status.Source.Min()) {
		status.Method = shared.SyncMethodDeleteMerge
		return nil
	}
	if !narrowInSyncSubset {
		return nil
	}

	narrowFilter := shared.CloneMap(filter)
	narrowFilter[idColumn] = criteria.NewLessOrEqual(status.Dest.Max())
	if d.isInSync(ctx, narrowFilter) {
		status.InSyncWithID = status.Dest.Max()
		status.Method = shared.SyncMethodInsert
		return nil
	}
	status.Method = shared.SyncMethodMerge
	idRange := data.NewIDRange(status.Source.Min(), status.Dest.Max())
	status.InSyncWithID, err = d.findMaxIDInSync(ctx, idRange, filter)
	return err
}

func (d *service) isInSync(ctx *shared.Context, filter map[string]interface{}) bool {
	candidate := &data.Status{}
	if source, dest, err := d.Fetch(ctx, filter); err == nil {
		if err = d.UpdateStatus(ctx, candidate, source, dest, filter, false); err == nil {
			return candidate.InSync
		}
	}
	return false
}

func (d *service) findMaxIDInSync(ctx *shared.Context, idRange *data.IDRange, filter map[string]interface{}) (int, error) {
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
		if d.isInSync(ctx, filter) {
			inSyncDestMaxID = candidateID
		}
		candidateID = idRange.Next(false)
	}
	return inSyncDestMaxID, nil
}

//NewService creates a new differ
func New(sync *model.Sync, dao dao.Service) *service {
	return &service{Sync: sync, dao: dao, Comparator: data.NewComparator(&sync.Diff)}
}
