package sync

import (
	"github.com/viant/dbsync/sync/strategy"
)

const (
	//DMLMerge regular MERGE DML
	DMLMerge = "merge"
	//DMLInsertReplace INSERT OR REPLACE DML
	DMLInsertReplace = "insertReplace"
	//DMLInsertUpddate INSERT ON DUPLICATE UPDATE DML style
	DMLInsertUpddate = "insertUpdate"
	//DMLInsert INSERT
	DMLInsert = "insert"
	//DMLDelete DELETE
	DMLDelete = "delete"
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

func (s *Strategy) Init() error {
	err := s.Diff.Init()
	if err == nil {
		if err = s.Partition.Init(); err == nil {
			err = s.Chunk.Init()
		}
	}
	return err
}
