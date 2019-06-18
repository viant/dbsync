package core

import (
	"dbsync/sync/shared"
	"sync"
	"sync/atomic"
	"time"
)

//Job represents db sync job
type Job struct {
	ID        string
	Error     string
	Status    string
	Progress  Progress
	Items     [] *Transferable
	Chunked   bool
	mutex     *sync.Mutex
	StartTime time.Time
	EndTime   *time.Time
}

func (j *Job) Update() {
	if len(j.Items) == 0 {
		return
	}
	sourceCount := 0
	destCount := 0
	transferred := 0
	for i := range j.Items {
		if j.Items[i].Status == nil {
			continue
		}
		sourceCount += j.Items[i].Source.Count()
		destCount += j.Items[i].Dest.Count()
		transferred += int(atomic.LoadUint32(&j.Items[i].Transferred))

	}
	j.Progress.Transferred = transferred
	j.Progress.SourceCount = sourceCount
	j.Progress.DestCount = destCount
	if sourceCount > 0 {
		j.Progress.Pct = transferred / sourceCount
	}
}

func (j *Job) Done(now time.Time) {
	if j.Status != shared.StatusError {
		j.Status = shared.StatusDone
	}
	j.EndTime = &now
}

func (j *Job) Add(transferable *Transferable) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.Items = append(j.Items, transferable)
}

//IsRunning returns true if jos has running status
func (j *Job) IsRunning() bool {
	return j.Status == shared.StatusRunning || j.EndTime == nil
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
