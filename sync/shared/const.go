package shared

const (
	//StatusOk ok status
	StatusOk = "ok"
	//StatusError error status
	StatusError = "error"
	//StatusDone done status
	StatusDone = "done"
	//StatusRunning sync1 running status
	StatusRunning = "running"

	//SyncModeBatch persistency mode,  batched each chunk/partition merged with tmp transient table, then after all chunk transferred temp table marged with dest table
	SyncModeBatch = "batch"
	//SyncModeIndividual individual - each chunk/partition merged with dest table,
	SyncModeIndividual = "individual"
	//TransientTableSuffix represents transient table suffix
	TransientTableSuffix = "_tmp"

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
	//DMLFilteredDelete DELETE
	DMLFilteredDelete = "filteredDelete"
	//TransientDMLDelete transient DELETE DML type
	TransientDMLDelete = "transientDelete"

	//SyncKindDirect direct king
	SyncKindDirect = "direct"

	//SyncKindInSync in sync
	SyncKindInSync = "inSync"

	//SyncMethodInsert insert sync1 method
	SyncMethodInsert = "insert"
	//SyncMethodDeleteMerge delete merge sync1 method
	SyncMethodDeleteMerge = "deleteMerge"
	//SyncMethodMerge merge sync1 method
	SyncMethodMerge = "merge"
	//SyncMethodDeleteInsert merge delete sync1 method
	SyncMethodDeleteInsert = "deleteInsert"
)
