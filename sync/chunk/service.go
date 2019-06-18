package chunk

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/criteria"
	"dbsync/sync/dao"
	"dbsync/sync/diff"
	"dbsync/sync/jobs"
	"dbsync/sync/merge"

	"dbsync/sync/contract/strategy"
	"dbsync/sync/shared"
	"dbsync/sync/transfer"
	"fmt"
)

//Service represents a chunk service
type Service interface {
	Build(tx *shared.Context) error
	Sync(tx *shared.Context) error
}

type service struct {
	*strategy.Strategy
	diff.Service
	job       jobs.Service
	partition *core.Partition
	dao       dao.Service
	Merger    merge.Service
	Transfer  transfer.Service
}

func (s *service) transferAndMerge(ctx *shared.Context, chunk *core.Chunk) error {
	isBatchMode := s.Chunk.SyncMode == shared.SyncModeBatch
	if s.DirectAppend && chunk.Transferable.Method == shared.SyncMethodInsert {
		chunk.Transferable.Suffix = ""
		chunk.IsDirect = true
	}

	request := s.Transfer.NewRequest(ctx, &chunk.Transferable)
	s.job.Get(ctx.ID).Add(&chunk.Transferable)
	err := s.Transfer.Post(ctx, request, &chunk.Transferable)
	if err != nil {
		return err
	}
	if chunk.IsDirect {
		return nil
	}
	transferable := chunk.Transferable.Clone()
	//Only merge/append can be batched
	if isBatchMode && ! transferable.ShouldDelete() {
		transferable.OwnerSuffix = s.partition.Suffix
		transferable.Method = shared.SyncMethodInsert
	}
	return s.Merger.Merge(ctx, transferable)
}

func (s *service) syncInBackground(ctx *shared.Context) {
	defer func() {
		s.partition.Chunks.Done()
	}()

	for {
		chunk := s.partition.Chunks.Take(ctx)
		if chunk == nil {
			return
		}
		if err := s.transferAndMerge(ctx, chunk); err != nil {
			//TODO handle error terminate partition sync, allow other to continue
			s.partition.SetError(err)
			return
		}

	}
}

func (s *service) mergeBatch(ctx *shared.Context) (err error) {
	transferable := s.partition.BatchTransferable()
	return s.Merger.Merge(ctx, transferable)
}

func (s *service) onSyncDone(ctx *shared.Context, err error) {
	defer s.partition.Close()
	isBatchMode := s.Chunk.SyncMode == shared.SyncModeBatch
	if isBatchMode {
		if err == nil {
			err = s.mergeBatch(ctx)
		}
	}
	s.partition.SetError(err)
}

func (s *service) Sync(ctx *shared.Context) (err error) {
	defer func() {
		s.onSyncDone(ctx, err)
	}()
	isBatchMode := s.Chunk.SyncMode == shared.SyncModeBatch
	if isBatchMode {
		if err = s.dao.RecreateTransientTable(ctx, s.partition.Suffix); err != nil {
			return err
		}
	}

	chunks := s.partition.Chunks
	threads := s.Strategy.Chunk.Threads
	if threads == 0 {
		threads = 1
	}

	for i := 0; i < threads; i++ {
		chunks.Add(1)
		go s.syncInBackground(ctx)
	}

	if err = s.Build(ctx); err != nil {
		return err
	}
	chunks.Wait()
	return err
}

//Build build partition chunks
func (s *service) Build(ctx *shared.Context) (err error) {

	offset := 0
	if s.partition.Status != nil {
		offset = s.partition.Status.Min()
	}
	maxID := s.partition.Status.Max()
	limit := s.Chunk.Size
	if limit == 0 {
		limit = 1
	}

	isLast := false
	for ; ! isLast; {
		var sourceSignature, destSignature *core.Signature
		sourceSignature, err = s.dao.ChunkSignature(ctx, contract.ResourceKindSource, offset, limit, s.partition.Filter)
		if err != nil {
			return err
		}
		isLast = sourceSignature.Count() != limit || sourceSignature.Count() == 0

		if ! isLast {
			destSignature, err = s.dao.ChunkSignature(ctx, contract.ResourceKindDest, offset, limit, s.partition.Filter)
		} else {
			filter := shared.CloneMap(s.partition.Filter)
			upperBound := offset + limit
			if maxID > upperBound {
				upperBound = maxID
			}
			filter[s.IDColumn()] = criteria.NewBetween(offset, upperBound)
			destSignature, err = s.dao.CountSignature(ctx, contract.ResourceKindDest, filter)
		}
		if err != nil {
			return err
		}

		status := core.NewStatus(sourceSignature, destSignature)
		if s.AppendOnly && sourceSignature.IsEqual(destSignature) {
			ctx.Log(fmt.Sprintf("chunk [%v .. %v] is inSyc", status.Min(), status.Max()))
			offset = status.Max() + 1
			continue
		}

		chunk, err := s.buildChunk(ctx, status, s.partition.Filter)
		if err != nil {
			return err
		}

		if chunk.InSync {
			ctx.Log(fmt.Sprintf("chunk [%v .. %v] is inSyc", status.Min(), status.Max()))
			offset = status.Max() + 1
			continue
		}
		if chunk.Status.InSyncWithID > 0 {
			chunk.SetMinID(s.IDColumn(), chunk.Status.InSyncWithID+1)
		}
		ctx.Log(fmt.Sprintf("chunk [%v .. %v] is outOfSync: %v (%v)\n", status.Min(), status.Max(), chunk.Method, chunk.Filter))
		s.partition.AddChunk(chunk)
		offset = status.Max() + 1
	}

	s.partition.CloseOffer()
	return nil
}

func (s *service) buildChunk(ctx *shared.Context, status *core.Status, filter map[string]interface{}) (*core.Chunk, error) {
	filter = shared.CloneMap(filter)
	filter[s.IDColumn()] = criteria.NewBetween(status.Min(), status.Max())
	source, dest, err := s.Fetch(ctx, filter)
	if err != nil {
		return nil, err
	}
	err = s.UpdateStatus(ctx, status, source, dest, filter, s.IsOptimized())
	return core.NewChunk(status, filter), err
}

//New creates a nwe chunk service
func New(sync *contract.Sync, partition *core.Partition, dao dao.Service, mutex *shared.Mutex, jobService jobs.Service, transferService transfer.Service) Service {
	return &service{
		partition: partition,
		Service:   diff.New(sync, dao),
		dao:       dao,
		Strategy:  &sync.Strategy,
		Merger:    merge.New(sync, dao, mutex),
		Transfer:  transferService,
		job:       jobService,
	}
}
