package shared


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
	TransientDMLDelete = "transientDelete"
)

