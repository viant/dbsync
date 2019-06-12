package chunk

import (
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

type Service interface {
	Build(tx *shared.Context) error
	Sync(tx *shared.Context) error
}

type service struct {
	*strategy.Strategy
	diff.Service
	partition *data.Partition
	dao       dao.Service
	Merger    merge.Service
	Transfer  transfer.Service
}

func (c *service) transferAndMerge(ctx *shared.Context, chunk *data.Chunk) error {
	isBatchMode := c.Chunk.SyncMode == shared.SyncModeBatch
	if c.DirectAppend &&  chunk.Transferable.Method == shared.SyncMethodInsert{
		chunk.Transferable.Suffix=""
		chunk.IsDirect = true
	}
	request := c.Transfer.NewRequest(ctx, &chunk.Transferable)
	err := c.Transfer.Post(ctx, request, &chunk.Transferable)
	if err != nil {
		return err
	}
	if chunk.IsDirect {
		return nil
	}
	transferable := chunk.Transferable
	//Only merge/append can be batched
	if isBatchMode && ! transferable.ShouldDelete() {
		transferable.OwnerSuffix = c.partition.Suffix
		transferable.Method = shared.SyncMethodInsert
	}
	return c.Merger.Merge(ctx, &transferable)
}



func (c *service) syncInBackground(ctx *shared.Context) {
	c.partition.Chunks.Done()
	for {
		chunk := c.partition.Chunks.Take(ctx)
		if chunk == nil {
			return
		}
		if err := c.transferAndMerge(ctx, chunk);err != nil {
			//TODO handle error terminate partition sync, allow other to continue
			c.partition.SetError(err)
			return
		}

	}
}

func (c *service) mergeBatch(ctx *shared.Context) (err error) {
	transferable := c.partition.BatchTransferable()
	if err = c.Merger.Merge(ctx, transferable); err == nil {
		_ = c.dao.DropTransientTable(ctx, c.partition.Suffix)
	}
	return err
}


func (c *service) Sync(ctx *shared.Context) (err error) {

	isBatchMode := c.Chunk.SyncMode == shared.SyncModeBatch
	if isBatchMode {
		if err = c.dao.CreateTransientTable(ctx, c.partition.Suffix);err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = c.mergeBatch(ctx)
			}
			if err != nil {
				c.partition.SetError(err)
			}
		}()
	}

	chunks := c.partition.Chunks
	defer chunks.Close()
	threads := c.Strategy.Chunk.Threads
	if threads == 0 {
		threads = 1
	}
	for i := 0; i < threads; i++ {
		chunks.Add(1)
		go c.syncInBackground(ctx)
	}
	if err = c.Build(ctx);err != nil {
		return err
	}
	chunks.Wait()
	return err
}



//Build build partition chunks
func (c *service) Build(ctx *shared.Context) (err error) {
	offset := 0
	if c.partition.Status != nil {
		offset = c.partition.Status.Min()
	}
	limit := c.Chunk.Size
	if limit == 0 {
		limit = 1
	}

	isLast := false
	for ; ! isLast; {
		var sourceSignature, destSignature *data.Signature
		sourceSignature, err = c.dao.ChunkSignature(ctx, model.ResourceKindSource, offset, limit, c.partition.Filter)
		if err != nil {
			return err
		}
		isLast = sourceSignature.Count() != limit || sourceSignature.Count() == 0

		if ! isLast {
			destSignature, err = c.dao.ChunkSignature(ctx, model.ResourceKindDest, offset, limit, c.partition.Filter)
		} else {
			filter := shared.CloneMap(c.partition.Filter)
			filter[c.IDColumn()] = criteria.NewBetween(offset, offset+limit)
			destSignature, err = c.dao.CountSignature(ctx, model.ResourceKindDest, filter)
		}
		if err != nil {
			return err
		}

		status := data.NewStatus(sourceSignature, destSignature)
		if c.AppendOnly && sourceSignature.IsEqual(destSignature) {
			ctx.Log(fmt.Sprintf("chunk [%v .. %v] is inSyc", status.Min(), status.Max()))
			continue
		}

		chunk, err := c.buildChunk(ctx, status, c.partition.Filter)
		if err != nil {
			return err
		}
		ctx.Log(fmt.Sprintf("chunk [%v .. %v] is outOfSync: %v (%v)\n", status.Min(), status.Max(), chunk.Method, chunk.Filter))
		c.partition.AddChunk(chunk)

		offset = status.Max() + 1
	}
	return nil
}

func (c *service) buildChunk(ctx *shared.Context, status *data.Status, filter map[string]interface{}) (*data.Chunk, error) {
	filter = shared.CloneMap(filter)
	filter[c.IDColumn()] = criteria.NewBetween(status.Min(), status.Max())
	source, dest, err := c.Fetch(ctx, filter)
	if err != nil {
		return nil, err
	}
	err = c.UpdateStatus(ctx, status, source, dest, filter, c.IsOptimized())
	return data.NewChunk(status, filter), err
}



//New creates a nwe chunk service
func New(sync *model.Sync, partition *data.Partition, dao dao.Service, mutex *shared.Mutex) *service {
	return &service{
		partition: partition,
		Service:   diff.New(sync, dao),
		dao:       dao,
		Strategy:  &sync.Strategy,
		Merger:    merge.New(sync, dao, mutex),
		Transfer:  transfer.New(sync, dao),
	}
}
