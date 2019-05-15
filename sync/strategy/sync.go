package strategy

const (
	//SyncModeBatch persistency mode,  batched each chunk/partition merged with tmp transient table, then after all chunk transferred temp table marged with dest table
	SyncModeBatch = "batch"
	//SyncModeIndividual individual - each chunk/partition merged with dest table,
	SyncModeIndividual = "individual"
)
