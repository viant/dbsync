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
	DiffDepth int `description:"controls detection of data that is similar"`
}

//Sync sync
type Sync struct {
	UniqueColumns []string
	DifferenceStrategy
	NumericPrecision int
	DateFormat       string
	DateLayout       string
	MergeStyle       string `description:"supported value:merge,insertReplace,insertUpdate,insertDelete"`
	Partition        PartitionInfo
	Force            bool `description:"if set skip checks if data in sync"`
	ChunkSQL         string
	ChunkSize        int `description:"chunk size in row count"`
	ChunkQueueSize   int
	DiffBatchSize    int
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
