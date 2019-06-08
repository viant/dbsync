package data

//Status data signature status
type Status struct {
	Source       *Signature
	Dest         *Signature
	InSync       bool
	isSubset     bool
	InSyncSubset *Status
}
