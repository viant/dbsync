package sync

import (
	"fmt"
	"github.com/viant/toolbox"
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

//PseudoColumn represents a pseudo column
type PseudoColumn struct {
	Expression string
	Name       string
}

//DiffColumn represents column expression for computing difference
type DiffColumn struct {
	Name             string
	Func             string
	Default          interface{}
	NumericPrecision int
	DateFormat       string
	DateLayout       string
	Alias            string
}

//DifferenceStrategy represents difference strategy
type DifferenceStrategy struct {
	Columns   []*DiffColumn
	CountOnly bool
	Depth     int `description:"controls detection of data that is similar"`
	BatchSize int
}

//ChunkSync represents chunk sync request part
type ChunkSync struct {
	SQL       string
	Size      int `description:"chunk size in row count"`
	QueueSize int
}

//Strategy sync strategy
type Strategy struct {
	Chunk     ChunkSync
	IDColumns []string
	Diff DifferenceStrategy
	NumericPrecision int
	DateFormat       string
	DateLayout       string
	MergeStyle       string `description:"supported value:merge,insertReplace,insertUpdate,insertDelete"`
	Partition        PartitionSync

	Force            bool `description:"if set skip checks if data in sync"`
}

//Expr returns expression
func (c *DiffColumn) Expr() string {
	column := c.Name
	if c.Default != nil {
		if toolbox.IsString(c.Default) {
			column = fmt.Sprintf("COALESCE(%v, '%v')", c.Name, c.Default)
		} else {
			column = fmt.Sprintf("COALESCE(%v, %v)", c.Name, c.Default)
		}
	}
	switch c.Func {
	case "COUNT":
		if column == "1" {
			return fmt.Sprintf("COUNT(%v) AS %v", column, c.Alias)
		}
		return fmt.Sprintf("COUNT(DISTINCT %v) AS %v", column, c.Alias)
	default:
		return fmt.Sprintf("%v(%v) AS %v", c.Func, column, c.Alias)
	}
}

func (s *Strategy) Init() error {
	if s.Diff.BatchSize == 0 {
		s.Diff.BatchSize = defaultDiffBatchSize
	}
	if s.NumericPrecision == 0 {
		s.NumericPrecision = 5
	}
	if s.DateFormat == "" && s.DateLayout == "" {
		s.DateLayout = toolbox.DateFormatToLayout("yyyy-MM-dd hh:mm:ss")
	} else if s.DateFormat != "" {
		s.DateLayout = toolbox.DateFormatToLayout(s.DateFormat)
	}
	if len(s.Partition.Columns) == 0 {
		s.Partition.Columns = make([]string, 0)
	}
	var threads = s.Partition.Threads
	if threads == 0 {
		s.Partition.Threads = 1
	}

	err := s.Chunk.Init()
	return err
}

func (c *ChunkSync) Init() error {
	if c.Size > 0 && c.QueueSize == 0 {
		c.QueueSize = 2
	}
	return nil
}
