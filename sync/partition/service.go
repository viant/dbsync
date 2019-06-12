package partition

import (
	"dbsync/sync/chunk"
	"dbsync/sync/criteria"
	"dbsync/sync/dao"
	"dbsync/sync/data"
	"dbsync/sync/diff"
	"dbsync/sync/merge"
	"dbsync/sync/model"
	"dbsync/sync/model/strategy"
	"dbsync/sync/shared"
	"dbsync/sync/transfer"
	"fmt"
)

//service represents partition
type service struct {
	Partitions *data.Partitions
	Mutex      *shared.Mutex
	*data.Comparator
	diff.Service
	Transfer transfer.Service
	Merger   merge.Service
	DbSync   *model.Sync
	*strategy.Strategy
	dao dao.Service
}

//Build build partition sync status
func (p *service) Build(ctx *shared.Context) error {
	if err := p.buildBatched(ctx); err != nil {
		return err
	}
	return p.buildIndividual(ctx)
}

func (p *service) Sync(ctx *shared.Context) (err error) {
	if p.Partitions == nil {
		return fmt.Errorf("partitions were empy")
	}
	switch p.Partition.SyncMode {
	case shared.SyncModeBatch:
		err = p.syncInBatches(ctx)
	default:
		err = p.syncIndividually(ctx, p.Partitions)
	}
	return err
}

func (p *service) syncInBatches(ctx *shared.Context) error {
	batchMap := criteria.NewBatchMap(p.Partition.BatchSize)
	_ = p.Partitions.Range(func(partition *data.Partition) error {
		if partition.Status.InSync {
			return nil
		}
		batchMap.Add(partition.Status.Method, partition.Filter)
		return nil
	})

	var partitionData = make([]*data.Partition, 0)
	counter := 0
	_ = batchMap.Range(func(key string, batch *criteria.Batch) error {
		items := batch.Get()
		for i := range items {
			partition := data.NewPartition(p.Strategy, items[i])
			partition.InitWithMethod(key, fmt.Sprintf("_%03d_%05d", counter, i))
			partitionData = append(partitionData, partition)
			if source, dest, err := p.Fetch(ctx, items[i]); err == nil {
				partition.Source = data.NewSignatureFromRecord(p.IDColumn(), source)
				partition.Dest = data.NewSignatureFromRecord(p.IDColumn(), dest)
			}
		}
		return nil
	})
	partitions := data.NewPartitions(partitionData, p.Strategy)
	return p.syncIndividually(ctx, partitions)
}

func (p *service) mergeBatch(ctx *shared.Context, partitions *data.Partitions) (err error) {
	transferable := partitions.BatchTransferable()
	if err = p.Merger.Merge(ctx, transferable); err == nil {
		_ = p.dao.DropTransientTable(ctx, shared.TransientTableSuffix)
	}
	return err
}

func (p *service) syncIndividually(ctx *shared.Context, partitions *data.Partitions) (err error) {
	isChunked := p.Chunk.Size > 0
	isBatched := p.Partition.SyncMode == shared.SyncModeBatch

	if isBatched {
		if err = p.dao.RecreateTransientTable(ctx, shared.TransientTableSuffix); err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = p.mergeBatch(ctx, partitions)
			}
		}()
	}

	//This run with multi go routines
	err = partitions.Range(func(partition *data.Partition) error {
		if isChunked {
			return p.syncPartitionChunks(ctx, partition)
		}
		return p.syncPartition(ctx, partition)
	})
	return err
}

func (p *service) syncPartitionChunks(ctx *shared.Context, partition *data.Partition) (err error) {
	chunker := chunk.New(p.DbSync, partition, p.dao, p.Mutex)
	if err = chunker.Init(); err != nil {
		return err
	}
	if err = chunker.Build(ctx); err != nil {
		return err
	}
	return chunker.Sync(ctx)
}

func (p *service) syncPartition(ctx *shared.Context, partition *data.Partition) (err error) {
	isBatchMode := p.Partition.SyncMode == shared.SyncModeBatch

	if p.DirectAppend && partition.Transferable.Method == shared.SyncMethodInsert {
		partition.Transferable.Suffix = ""
		partition.IsDirect = true
	}
	request := p.Transfer.NewRequest(ctx, &partition.Transferable)
	if err = p.Transfer.Post(ctx, request, &partition.Transferable); err != nil {
		return err
	}
	if partition.IsDirect {
		return nil
	}
	transferable := partition.Transferable
	//Only merge/append can be batched
	if isBatchMode && ! transferable.ShouldDelete() {
		transferable.OwnerSuffix = shared.TransientTableSuffix
		transferable.Method = shared.SyncMethodInsert
	}
	return p.Merger.Merge(ctx, &transferable)
}

func (p *service) buildBatched(ctx *shared.Context) (err error) {
	if ! p.IsOptimized() {
		return nil
	}
	partitionsCriteria := p.Partitions.Criteria()
	for i := range partitionsCriteria {
		if err := p.buildBatch(ctx, partitionsCriteria[i]); err != nil {
			break
		}
	}
	return err
}

//build build status to all  partition, ideally al should be done in batch
func (p *service) buildIndividual(ctx *shared.Context) (err error) {
	return p.Partitions.Range(func(partition *data.Partition) error {
		if partition.Status != nil {
			return nil
		}
		source, dest, err := p.Fetch(ctx, partition.Filter)
		if err != nil {
			return err
		}
		partition.Status, err = p.Check(ctx, source, dest, partition.Filter)
		return err
	})
}

func (p *service) buildBatch(ctx *shared.Context, filter map[string]interface{}) error {
	sourceSignatures, err := p.dao.Signatures(ctx, model.ResourceKindSource, filter)
	if err != nil {
		return err
	}
	if len(sourceSignatures) == 0 {
		return nil //nothing to sync
	}
	destSignatures, err := p.dao.Signatures(ctx, model.ResourceKindDest, filter)
	if err != nil {
		return err
	}
	data.AlignRecords(sourceSignatures, destSignatures)
	index, err := p.match(ctx, sourceSignatures, destSignatures)
	if err != nil {
		return err
	}
	if err = p.validate(ctx, index); err != nil {
		return err
	}

	for key := range index.Source {
		source := index.Source[key]
		dest := index.Dest[key]
		partition := p.Partitions.Get(key)
		if partition == nil {
			continue
		}
		if partition.Status, err = p.Check(ctx, source, dest, partition.Filter); err != nil {
			return err
		}
	}
	return nil
}

func (p *service) validate(ctx *shared.Context, index *data.Index) error {
	for key := range index.Source {
		source := index.Source[key]
		dest := index.Dest[key]
		if err := p.Partitions.Validate(ctx, p.Comparator, source, dest); err != nil {
			return err
		}
	}
	return nil

}

func (p *service) match(ctx *shared.Context, source, dest data.Records) (*data.Index, error) {
	result := data.NewIndex()
	if len(source) == 1 {
		result.Source[""] = source[0]
		result.Dest[""] = dest[0]
		return result, nil
	}
	indexer := data.NewIndexer(p.Partition.Columns)
	index := indexer.Index(source, dest)
	if hasDest := len(index.Dest) > 0; hasDest {
		return index, nil
	}

	//if no even one batch dest has been found, try to get individual dest
	for key := range index.Source {
		partition := p.Partitions.Get(key)
		if partition == nil {
			continue
		}
		if dest, _ := p.dao.Signatures(ctx, model.ResourceKindDest, partition.Filter); len(dest) == 1 {
			index.Dest[key] = dest[0]
		}
	}
	return index, nil
}

func (p *service) Init(ctx *shared.Context) error {
	if p.Strategy == nil {
		return fmt.Errorf("strategy was empty")
	}
	return p.loadPartitions(ctx)
}

func (p *service) loadPartitions(ctx *shared.Context) error {
	var source = make([]*data.Partition, 0)
	if p.Partition.ProviderSQL != "" {
		values, err := p.dao.Partitions(ctx, model.ResourceKindSource)
		if err != nil {
			return err
		}
		for i := range values {
			source = append(source, data.NewPartition(p.Strategy, values[i]))
		}
	}

	//TODO load dest partition if SQL defined and detect missing (removed from source - to remove also in dest if present)
	p.Partitions = data.NewPartitions(source, p.Strategy)
	return nil
}

//New creates new partition service
func New(sync *model.Sync, dao dao.Service, mutex *shared.Mutex) *service {
	return &service{
		dao:        dao,
		Service:    diff.New(sync, dao),
		Comparator: data.NewComparator(&sync.Diff),
		Merger:     merge.New(sync, dao, mutex),
		Transfer:   transfer.New(sync, dao),
		DbSync:     sync,
		Strategy:   &sync.Strategy,
		Mutex:      mutex,
	}
}
