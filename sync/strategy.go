package sync

import (
	"dbsync/sync/strategy"
)

const (
	//DMLMerge regular MERGE DML
	DMLMerge = "merge"
	//DMLMergeInto regular MERGE DML
	DMLMergeInto = "mergeInto"
	//DMLInsertOrReplace INSERT OR REPLACE DML
	DMLInsertOrReplace = "insertOrReplace"
	//DMLInsertOnDuplicateUpddate INSERT ON DUPLICATE UPDATE DML style
	DMLInsertOnDuplicateUpddate = "insertOnDuplicateUpdate"

	//DMLInsertOnConflictUpddate INSERT ON CONFLICT DO UPDATE DML style
	DMLInsertOnConflictUpddate = "insertOnConflictUpdate"
	//DMLInsert INSERT
	DMLInsert = "insert"
	//DMLDelete DELETE
	DMLDelete = "delete"

	//DMLDelete DELETE
	transientDMLDelete = "transientDelete"
)

//Strategy represnet a sync strategy
type Strategy struct {
	Chunk      strategy.Chunk
	IDColumns  []string
	Diff       strategy.Diff
	MergeStyle string `description:"supported value:merge,insertReplace,insertUpdate,insertDelete"`
	Partition  strategy.Partition
	AppendOnly bool `description:"if set instead of merge, insert will be used"`
	Force      bool `description:"if set skip checks if data in sync"`
}

//Init initializes strategy
func (s *Strategy) Init() error {
	err := s.Diff.Init()
	if err == nil {
		if err = s.Partition.Init(); err == nil {
			err = s.Chunk.Init()
		}
	}
	return err
}
