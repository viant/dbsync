package jobs

import (
	"dbsync/sync/core"
	"sort"
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
	sort.Sort(r)
	r.Jobs = append(r.Jobs, job)
}

//Len implements sort Len interface
func (r registry) Len() int { return len(r.Jobs) }

//Swap implements sort Swap interface
func (r registry) Swap(i, j int) { r.Jobs[i], r.Jobs[j] = r.Jobs[j], r.Jobs[i] }

//Less implements sort Less interface
func (r registry) Less(i, j int) bool {
	return r.Jobs[i].StartTime.Before(r.Jobs[j].StartTime)
}

//newRegistry returns new transfers
func newRegistry() *registry {
	return &registry{
		Mutex: &sync.RWMutex{},
		Jobs:  make([]*core.Job, 0),
	}
}
