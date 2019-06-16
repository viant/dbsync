package partition

import (
	"dbsync/sync/chunk"
	"dbsync/sync/core"
	"dbsync/sync/criteria"
	"dbsync/sync/dao"
	"dbsync/sync/diff"
	"dbsync/sync/history"
	"dbsync/sync/jobs"
	"dbsync/sync/merge"
	"dbsync/sync/model"
	"dbsync/sync/model/strategy"
	"dbsync/sync/shared"
	"dbsync/sync/transfer"
	"fmt"
)


type Service interface {

	Build(ctx *shared.Context) error

	Sync(ctx *shared.Context) error

}


//service represents partition
type service struct {
	Partitions *core.Partitions
	Mutex      *shared.Mutex
	*core.Comparator
	diff.Service
	Transfer transfer.Service
	Merger   merge.Service
	DbSync   *model.Sync
	job      jobs.Service
	history  history.Service
	*strategy.Strategy
	dao dao.Service
}

//Build build partition sync status
func (s *service) Build(ctx *shared.Context) error {
	if err := s.buildBatched(ctx); err != nil {
		return err
	}
	return s.buildIndividual(ctx)
}

func (s *service) Sync(ctx *shared.Context) (err error) {
	if s.Partitions == nil {
		return fmt.Errorf("partitions were empy")
	}
	switch s.Partition.SyncMode {
	case shared.SyncModeBatch:
		err = s.syncInBatches(ctx)
	default:
		err = s.syncIndividually(ctx, s.Partitions)
	}
	return err
}

func (s *service) syncInBatches(ctx *shared.Context) error {
	batchMap := criteria.NewBatchMap(s.Partition.BatchSize)
	_ = s.Partitions.Range(func(partition *core.Partition) error {
		if partition.Status.InSync {
			return nil
		}
		batchMap.Add(partition.Status.Method, partition.Filter)
		return nil
	})

	var partitionData = make([]*core.Partition, 0)
	_ = batchMap.Range(func(key string, batch *criteria.Batch) error {
		items := batch.Get()
		for i := range items {
			partition := core.NewPartition(s.Strategy, items[i])
			partition.InitWithMethod(key, fmt.Sprintf("_%s%05d", key[0:1], i+1))
			partitionData = append(partitionData, partition)
			if source, dest, err := s.Fetch(ctx, items[i]); err == nil {
				partition.Source = core.NewSignatureFromRecord(s.IDColumn(), source)
				partition.Dest = core.NewSignatureFromRecord(s.IDColumn(), dest)
			}
		}
		return nil
	})
	partitions := core.NewPartitions(partitionData, s.Strategy)
	return s.syncIndividually(ctx, partitions)
}

func (s *service) mergeBatch(ctx *shared.Context, partitions *core.Partitions) (err error) {
	transferable := partitions.BatchTransferable()
	return s.Merger.Merge(ctx, transferable);
}

func (s *service) syncIndividually(ctx *shared.Context, partitions *core.Partitions) (err error) {
	isChunked := s.Chunk.Size > 0
	isBatched := s.Partition.SyncMode == shared.SyncModeBatch

	if isBatched {
		if err = s.dao.RecreateTransientTable(ctx, shared.TransientTableSuffix); err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = s.mergeBatch(ctx, partitions)
			}
		}()
	}

	//This run with multi go routines
	err = partitions.Range(func(partition *core.Partition) error {
		if partition.Status == nil || partition.InSync {
			return nil
		}
		if partition.InSync {
			return nil
		}
		if isChunked {
			return s.syncPartitionChunks(ctx, partition)
		}
		return s.syncPartition(ctx, partition)
	})
	return err
}

func (s *service) syncPartitionChunks(ctx *shared.Context, partition *core.Partition) (err error) {
	chunker := chunk.New(s.DbSync, partition, s.dao, s.Mutex, s.job)
	if err = chunker.Init(); err != nil {
		return err
	}
	if err = chunker.Build(ctx); err != nil {
		return err
	}
	return chunker.Sync(ctx)
}

func (s *service) syncPartition(ctx *shared.Context, partition *core.Partition) (err error) {
	isBatchMode := s.Partition.SyncMode == shared.SyncModeBatch

	if s.DirectAppend && partition.Transferable.Method == shared.SyncMethodInsert {
		partition.Transferable.Suffix = ""
		partition.IsDirect = true
	}
	request := s.Transfer.NewRequest(ctx, &partition.Transferable)
	s.job.Get(ctx.ID).Add(&partition.Transferable)
	if err = s.Transfer.Post(ctx, request, &partition.Transferable); err != nil {
		return err
	}
	if partition.IsDirect {
		return nil
	}
	transferable := partition.Transferable.Clone()
	//Only merge/append can be batched
	if isBatchMode && ! transferable.ShouldDelete() {
		transferable.OwnerSuffix = shared.TransientTableSuffix
		transferable.Method = shared.SyncMethodInsert
	}
	return s.Merger.Merge(ctx, transferable)
}

func (s *service) buildBatched(ctx *shared.Context) (err error) {
	if ! s.IsOptimized() {
		return nil
	}
	partitionsCriteria := s.Partitions.Criteria()
	for i := range partitionsCriteria {
		if err := s.buildBatch(ctx, partitionsCriteria[i]); err != nil {
			break
		}
	}
	return err
}

//build build status to all  partition, ideally al should be done in batch
func (s *service) buildIndividual(ctx *shared.Context) (err error) {
	return s.Partitions.Range(func(partition *core.Partition) error {
		if partition.Status != nil {
			return nil
		}
		source, dest, err := s.Fetch(ctx, partition.Filter)
		if err != nil {
			return err
		}
		if partition.Status, err = s.Check(ctx, source, dest, partition.Filter); err == nil {
			partition.Init()
		}
		return err
	})
}

func (s *service) buildBatch(ctx *shared.Context, filter map[string]interface{}) error {
	sourceSignatures, err := s.dao.Signatures(ctx, model.ResourceKindSource, filter)
	if err != nil {
		return err
	}
	if len(sourceSignatures) == 0 {
		return nil //nothing to sync
	}
	destSignatures, err := s.dao.Signatures(ctx, model.ResourceKindDest, filter)
	if err != nil {
		return err
	}
	core.AlignRecords(sourceSignatures, destSignatures)
	index, err := s.match(ctx, sourceSignatures, destSignatures)
	if err != nil {
		return err
	}
	if err = s.validate(ctx, index); err != nil {
		return err
	}

	for key := range index.Source {
		source := index.Source[key]
		dest := index.Dest[key]

		partition := s.Partitions.Get(key)
		if partition == nil {
			continue
		}
		partition.Status, err = s.Check(ctx, source, dest, partition.Filter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) validate(ctx *shared.Context, index *core.Index) error {
	for key := range index.Source {
		source := index.Source[key]
		dest := index.Dest[key]
		if err := s.Partitions.Validate(ctx, s.Comparator, source, dest); err != nil {
			return err
		}
	}
	return nil

}

func (s *service) match(ctx *shared.Context, source, dest core.Records) (*core.Index, error) {
	result := core.NewIndex()
	if len(source) == 1 {
		result.Source[""] = source[0]
		result.Dest[""] = dest[0]
		return result, nil
	}
	indexer := core.NewIndexer(s.Partition.Columns)
	index := indexer.Index(source, dest)
	if hasDest := len(index.Dest) > 0; hasDest {
		return index, nil
	}

	//if no even one batch dest has been found, try to get individual dest
	for key := range index.Source {
		partition := s.Partitions.Get(key)
		if partition == nil {
			continue
		}
		if dest, _ := s.dao.Signatures(ctx, model.ResourceKindDest, partition.Filter); len(dest) == 1 {
			index.Dest[key] = dest[0]
		}
	}
	return index, nil
}

func (s *service) Init(ctx *shared.Context) error {
	if s.Strategy == nil {
		return fmt.Errorf("strategy was empty")
	}
	return s.loadPartitions(ctx)
}

func (s *service) loadPartitions(ctx *shared.Context) error {
	var source = make([]*core.Partition, 0)
	if s.Partition.ProviderSQL != "" {
		values, err := s.dao.Partitions(ctx, model.ResourceKindSource)
		if err != nil {
			return err
		}
		for i := range values {
			source = append(source, core.NewPartition(s.Strategy, values[i]))
		}
	}

	//TODO load dest partition if SQL defined and detect missing (removed from source - to remove also in dest if present)
	s.Partitions = core.NewPartitions(source, s.Strategy)
	s.Partitions.Init()
	return nil
}

//New creates new partition service
func New(sync *model.Sync, dao dao.Service, mutex *shared.Mutex, jobbService jobs.Service, historyService history.Service) *service {
	return &service{
		dao:        dao,
		Service:    diff.New(sync, dao),
		Comparator: core.NewComparator(&sync.Diff),
		Merger:     merge.New(sync, dao, mutex),
		Transfer:   transfer.New(sync, dao),
		DbSync:     sync,
		Strategy:   &sync.Strategy,
		Mutex:      mutex,
		job:        jobbService,
		history:    historyService,
	}
}
