package data

import (
	"time"
)

//TransferJob represents transfer job
type TransferJob struct {
	Id     int
	Error  string
	Status string
	*Transferable
	StartTime time.Time
	EndTime   *time.Time
}
