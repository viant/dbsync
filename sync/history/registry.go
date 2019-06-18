package history

import (
	"sync"
)

//registry represents a registry
type registry struct {
	maxHistory int
	registry   map[string][]*Job
	mux        *sync.Mutex
}

//Get returns stats for supplied id
func (r *registry) get(id string) []*Job {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.registry[id]
}

//List returns job info for
func (r *registry) list(max int) map[string][]*Job {
	r.mux.Lock()
	defer r.mux.Unlock()
	if max == 0 {
		max = 1
	}
	result := make(map[string][]*Job)
	for k := range r.registry {
		if len(r.registry[k]) >max {
			result[k] = r.registry[k][:max]
			continue
		}
		result[k] = r.registry[k]
	}
	return result
}

//register register a job
func (r *registry) register(job *Job) {
	r.mux.Lock()
	defer r.mux.Unlock()
	_, ok := r.registry[job.ID]
	if ! ok {
		r.registry[job.ID] = make([]*Job, 0)
	}

	length := len(r.registry[job.ID])
	 if length+1 >= r.maxHistory {
		r.registry[job.ID] = r.registry[job.ID][:r.maxHistory-1]
	}
	r.registry[job.ID] = append([]*Job{job}, r.registry[job.ID]...)
}

//newRegistry creates a new registry
func newRegistry(maxHistory int) *registry {
	if maxHistory == 0 {
		maxHistory = 1
	}
	return &registry{
		maxHistory: maxHistory,
		registry:   make(map[string][]*Job),
		mux:        &sync.Mutex{},
	}
}
