package sync

const (
	//Regular MERGE DML
	DMLMerge = "merge"
	//INSERT OR REPLACE DML
	DMLInsertReplace = "insertReplace"
	//INSERT ON DUPLICATE UPDATE DML style
	DMLInsertUpddate = "insertUpdate"
	//INSERT DELETE
	DMLInsertDelete = "insertDelete"
	//INSERT
	DMLInsert = "insert"
	DMLDelete = "delete"
)

//PseudoColumn represents a pseudo column
type PseudoColumn struct {
	Expression string
	Name       string
}

//Sync sync
type Sync struct {
	UniqueColumns     []string
	CustomDiffColumns []string
	NumericPrecision  int
	MergeStyle        string `description:"supported value:merge,insertReplace,insertUpdate,insertDelete"`
	Partition         Partition
	Full              bool
	ChunkSQL          string
	ChunkRowCount     int
	MultiChunk        int
}
