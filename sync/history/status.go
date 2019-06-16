package history

import "dbsync/sync/shared"

//Represents history status
type Status struct {
	Status string
	Error string
	Errors map[string]string
	Transferred map[string]int
}


func NewStatus() * Status{
	return &Status{
		Status:shared.StatusOk,
		Errors:make(map[string]string),
		Transferred:make(map[string]int),
	}
}