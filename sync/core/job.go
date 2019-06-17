package core

import (
	"dbsync/sync/shared"
	"sync"
	"time"
)

//Job represents db sync job
type Job struct {
	ID          string
	Error       string
	Status      string
	Items       [] *Transferable
	Chunked     bool
	mutex       *sync.Mutex
	StartTime   time.Time
	EndTime     *time.Time
}

func (j *Job) Done(now time.Time) {
	j.Status = shared.StatusDone
	j.EndTime = &now
}

func (j *Job) Add(transferable *Transferable) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.Items = append(j.Items, transferable)
}

//IsRunning returns true if jos has running status
func (j *Job) IsRunning() bool {
	return j.Status == shared.StatusRunning
}

//NewJob creates a new job
func NewJob(id string) *Job {
	return &Job{
		ID:        id,
		StartTime: time.Now(),
		mutex:     &sync.Mutex{},
		Items:     make([]*Transferable, 0),
	}
}
