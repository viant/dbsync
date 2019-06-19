package history

import (
	"dbsync/sync/shared"
	"time"
)

//Status represents a service status
type Status struct {
	Status       string
	Error        string
	Errors       map[string]string
	Transferred  map[string]int
	LastSyncTime *time.Time
	UpTime       string
}

//NewStatus creates a new status
func NewStatus() *Status {
	return &Status{
		Status:      shared.StatusOk,
		Errors:      make(map[string]string),
		Transferred: make(map[string]int),
	}
}
