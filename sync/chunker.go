package sync

import (
	"fmt"
	"github.com/viant/dsc"
)

type chunker struct {
	limit           int
	max             int
	idColumn        string
	session         *Session
	partition       *Partition
	source          *ChunkInfo
	dest            *ChunkInfo
	sourceSQL       string
	destSQL         string
	optimizeSync    bool
	hasOverflowData bool
	countOnly       bool
}

func (c *chunker) reset() {
	c.dest = nil
	c.destSQL = ""
	c.source = nil
	c.sourceSQL = ""
}

func (c *chunker) readInfo(manager dsc.Manager, DQL string) (*ChunkInfo, error) {
	result := &ChunkInfo{}
	if _, err := manager.ReadSingle(result, DQL, nil, nil); err != nil {
		c.session.Error(c.partition, fmt.Sprintf("failed run source chunk SQL: %v, %v\n", DQL, err))
		return nil, err
	}
	return result, nil
}

func (c *chunker) readChunkByIDAndLimit(resource *Resource) (*ChunkInfo, string, error) {
	criteria := c.partition.CloneCriteria()
	criteria[c.idColumn] = &greaterOrEqual{c.max}
	manager := c.session.SourceDB
	if c.session.Dest == resource {
		manager = c.session.DestDB
	}
	DQL, err := c.session.Builder.ChunkDQL(resource, c.max, c.limit, criteria)
	if err != nil {
		return nil, "", err
	}
	result, err := c.readInfo(manager, DQL)
	if err != nil {
		return result, DQL, err
	}
	if err = result.Validate(DQL, c.limit); err != nil {
		return result, DQL, err
	}
	return result, DQL, err
}

func (c *chunker) inSync() bool {
	return c.dest.Count() == c.source.Count() &&
		c.dest.Max() == c.source.Max() &&
		c.dest.Min() == c.source.Min()
}

func (c *chunker) idRangeCriteria(min, max int) Criteria {
	criteria := c.partition.CloneCriteria()
	criteria[c.idColumn] = &between{from: min, to: max}
	return criteria
}

func (c *chunker) readChunkByIDRange(min, max int) error {
	criteria := c.idRangeCriteria(min, max)
	c.destSQL = c.session.Builder.CountDQL("", c.session.Dest, criteria)
	var err error
	c.dest, err = c.readInfo(c.session.DestDB, c.destSQL)
	return err
}

func (c *chunker) readSource() error {
	var err error
	c.source, c.sourceSQL, err = c.readChunkByIDAndLimit(c.session.Source)
	return err
}

func (c *chunker) readDest(min, max int) error {
	if c.hasOverflowData || (c.source.Count() != c.limit) {
		var err error
		c.dest, c.destSQL, err = c.readChunkByIDAndLimit(c.session.Dest)
		return err
	}
	return c.readChunkByIDRange(min, max)
}

func (c *chunker) getChunkLowerBound() int {
	min := c.source.Min()
	firstPartitionChunk := len(c.partition.chunks) == 0
	if firstPartitionChunk && c.partition.Info != nil && c.partition.MinValue < min && min != 0 {
		c.session.Log(c.partition, fmt.Sprintf("updating chunk min with partition narrowed min: %v\n", c.partition.MinValue))
		return c.partition.MinValue
	}
	return min
}

func (c *chunker) getChunkUpperBound() int {
	if c.source.Max() < c.dest.Max() {
		c.session.Log(c.partition, fmt.Sprintf("updating chunk max: %v\n", c.dest.Max()))
		return c.dest.Max()
	}
	return c.source.Max()
}

func (c *chunker) hasMore() bool {
	return (c.source.Count() != 0 || c.dest.Count() != 0) &&
		(c.source.Max() <= c.partition.SourceMax) //in case of constantly changing source data chanking would never end without this condition
}

func newChunker(session *Session, partition *Partition) *chunker {
	max := 0
	if partition.SyncFromID > 0 { //data is sync upto SyncFromID
		max = partition.SyncFromID + 1
	}
	return &chunker{
		session:         session,
		partition:       partition,
		idColumn:        session.Builder.IDColumns[0],
		limit:           session.Request.Chunk.Size,
		max:             max,
		optimizeSync:    !session.Request.Force,
		hasOverflowData: partition.Method == SyncMethodDeleteMerge,
		countOnly:       session.Request.Diff.CountOnly,
	}
}

func (c *chunker) build(session *Session, partition *Partition) error {
	var err error
	for ; ; c.reset() {
		i := len(partition.chunks)
		if err = c.readSource(); err != nil {
			return err
		}
		minValue := c.getChunkLowerBound()
		maxValue := c.source.Max()
		if err = c.readDest(minValue, maxValue); err != nil {
			return err
		}
		if !c.hasMore() {
			break
		}
		maxValue = c.getChunkUpperBound()
		criteria := c.idRangeCriteria(minValue, maxValue)
		c.max = maxValue + 1

		session.Log(partition, fmt.Sprintf("sourceDQL: %v", c.sourceSQL))
		session.Log(partition, fmt.Sprintf("destSQL: %v", c.destSQL))
		session.Log(partition, fmt.Sprintf("chunk[%d]: [%v..%v] is chunkInSync: %v", i, minValue, maxValue, c.inSync()))

		if c.optimizeSync && c.inSync() {
			if c.countOnly {
				continue
			}
			info, _ := session.GetSyncInfo(criteria, true)
			session.Log(partition, fmt.Sprintf("chunk[%d]: [%v..%v] in sync: %v", i, minValue, maxValue, info.InSync))
			if info.InSync {
				continue
			}
			if info.SyncFromID > 0 {
				minValue = info.SyncFromID
				criteria = c.idRangeCriteria(minValue, maxValue)
				session.Log(partition, fmt.Sprintf("updating chunk narrowed min: %v -> %v\n", info.SyncFromID, info.Method))
			}
		}

		partition.WaitGroup.Add(1)
		chunk := newChunk(c.source, c.dest, minValue, maxValue, partition, criteria)
		session.Log(partition, fmt.Sprintf("chunk[%d]: sync method: %v [%v..%v] is syncing ...\n\tcount:(%d:%d), max:(%d:%d)", i, chunk.Method,
			c.source.Min(), c.dest.Max(),
			c.source.Count(), c.dest.Count(),
			c.source.Max(), c.dest.Max()))
		partition.AddChunk(chunk)
	}
	return nil
}
