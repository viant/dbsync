package sync

import (
	"sort"
	"sync"
	"time"
)

//Job represents a sync job
type Job struct {
	ID        string
	StartTime time.Time
	EndTime   *time.Time
	Elapsed   time.Duration
	Method    string
	*Transfers
	Status   string
	Stage    string
	Error    string
	Progress Progress
}

//Update update job state
func (j *Job) Update() {
	if j.EndTime == nil {
		j.Elapsed = time.Now().Sub(j.StartTime)
	} else {
		j.Elapsed = j.EndTime.Sub(j.StartTime)
	}
	destCount := 0
	sourceCount := 0
	transffered := 0
	transfers := j.Transfers.Transfers
	for _, transfer := range transfers {
		destCount += int(transfer.DestCount)
		sourceCount += transfer.SourceCount
		transffered += int(transfer.Transferred)
	}
	j.Progress.SourceCount = sourceCount
	j.Progress.DestCount = destCount
	j.Progress.SetTransferredCount(transffered)

}

//NewJob creates a new job
func NewJob(ID string) *Job {
	return &Job{
		ID:        ID,
		StartTime: time.Now(),
		Transfers: NewTransfers(),
		Status:    StatusRunning,
	}
}

//Jobs represents job collections
type Jobs struct {
	mutex *sync.RWMutex
	jobs  []*Job
}

//Len sorts interface Len
func (js Jobs) Len() int { return len(js.jobs) }

//Swap sorts interface Swap
func (js Jobs) Swap(i, j int) { js.jobs[i], js.jobs[j] = js.jobs[j], js.jobs[i] }

//Less sorts interface Less
func (js Jobs) Less(i, j int) bool { return js.jobs[i].StartTime.Before(js.jobs[j].StartTime) }

//Add adds a job
func (js *Jobs) Add(job *Job) {
	js.pruneJobs()
	js.mutex.Lock()
	defer js.mutex.Unlock()
	js.jobs = append(js.jobs, job)
}

//List lists jobs
func (js *Jobs) List() []*Job {
	js.pruneJobs()
	js.mutex.Lock()
	defer js.mutex.Unlock()
	sort.Sort(js)
	for _, item := range js.jobs {
		item.Update()
	}
	return js.jobs
}

//Get returns a job by ID
func (js *Jobs) Get(id string) *Job {
	js.pruneJobs()
	js.mutex.RLock()
	defer js.mutex.RUnlock()
	for _, candidate := range js.jobs {
		if candidate.ID == id {
			candidate.Update()
			return candidate
		}
	}
	return nil
}

func (js *Jobs) pruneJobs() {
	js.mutex.Lock()
	defer js.mutex.Unlock()
	sort.Sort(js)
	temp := js.jobs
	js.jobs = make([]*Job, 0)
	for _, candidate := range temp {
		if candidate.Status == StatusError || candidate.Status == StatusDone {
			if candidate.EndTime != nil && time.Now().Sub(*candidate.EndTime) > time.Minute {
				continue
			}
		}
		js.jobs = append(js.jobs, candidate)
	}
}

//NewJobs creates new jobs
func NewJobs() *Jobs {
	return &Jobs{
		jobs:  make([]*Job, 0),
		mutex: &sync.RWMutex{},
	}
}
