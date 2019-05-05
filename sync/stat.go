package sync

import (
	"sync"
	"time"
)

//Stats represents past sync statistics
type Stats struct {
	maxHistory int
	ID         string
	Stats      []*RunStat
	mux        *sync.Mutex
}

//StatRegistry represents stats registry
type StatRegistry struct {
	maxHistory int
	registry   map[string]*Stats
	mux        *sync.Mutex
}

//RunStat represents sync stats
type RunStat struct {
	StartTime            time.Time
	EndTime              time.Time
	TimeTakenMs          int
	SourceCount          int
	DestCount            int
	Transferred          int
	Methods              map[string]int
	PartitionTransferred int
	Error                string
}

//Add add sync stat
func (s *Stats) Add(syncStat *RunStat) {
	s.mux.Lock()
	defer s.mux.Unlock()
	var stats = make([]*RunStat, 0)
	stats = append(stats, syncStat)
	for i := 0; i < len(s.Stats); i++ {
		if len(stats) >= s.maxHistory {
			break
		}
		stats = append(stats, s.Stats[i])
	}
	s.Stats = stats
}

//NewSyncStat creates a new stats for
func NewSyncStat(job *Job) *RunStat {
	job.Update()
	run := &RunStat{
		StartTime:   job.StartTime,
		EndTime:     job.StartTime.Add(job.Elapsed),
		TimeTakenMs: int(job.Elapsed / time.Millisecond),
		SourceCount: job.Progress.SourceCount,
		DestCount:   job.Progress.DestCount,
		Transferred: int(job.Progress.Transferred),
		Error:       job.Error,
		Methods:     make(map[string]int),
	}
	return run
}

//Get returns stats for supplied id
func (r *StatRegistry) Get(id string) *Stats {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.registry[id]
}

//GetOrCreate get or creates and get stats for supplied id
func (r *StatRegistry) GetOrCreate(id string) *Stats {
	r.mux.Lock()
	defer r.mux.Unlock()
	result, ok := r.registry[id]
	if ok {
		return result
	}
	result = NewStats(id, r.maxHistory)
	r.registry[id] = result
	return result
}

//NewStats creates a new stats
func NewStats(id string, maxHistory int) *Stats {
	return &Stats{
		mux:        &sync.Mutex{},
		Stats:      make([]*RunStat, 0),
		ID:         id,
		maxHistory: maxHistory,
	}
}

//NewStatRegistry creates a new registry
func NewStatRegistry(maxHistory int) *StatRegistry {
	return &StatRegistry{
		maxHistory: maxHistory,
		registry:   make(map[string]*Stats),
		mux:        &sync.Mutex{},
	}
}
