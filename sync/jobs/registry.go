package jobs

import (
	"dbsync/sync/core"
	"sync"
	"time"
)

var pruneThreshold = time.Minute

//registry represents transfers
type registry struct {
	Jobs  []*core.Job
	Mutex *sync.RWMutex
}

//List lists all jobs
func (r *registry) list() []*core.Job {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	jobs := r.Jobs
	return jobs
}


func (r *registry) prune() {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	jobs := r.Jobs
	if len(jobs) == 0 {
		return
	}
	var newJobs = make([]*core.Job, 0)

	for i := range jobs {
		if jobs[i].IsRunning() {
			newJobs = append(newJobs, jobs[i])
			continue
		}
		if jobs[i].EndTime.Sub(time.Now()) < pruneThreshold {
			newJobs = append(newJobs, jobs[i])
		}
	}
	r.Jobs = newJobs
}


//add adds transfer job
func (r *registry) add(job *core.Job) {
	r.prune()
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	newJobs := make([]*core.Job, 1)
	newJobs[0] = job
	for i := range r.Jobs {
		if r.Jobs[i].ID == job.ID {
			continue
		}
		newJobs = append(newJobs, r.Jobs[i])
	}
	r.Jobs = newJobs
}


//newRegistry returns new transfers
func newRegistry() *registry {
	return &registry{
		Mutex: &sync.RWMutex{},
		Jobs:  make([]*core.Job, 0),
	}
}
