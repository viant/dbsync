package history

import (
	"dbsync/sync/core"
	"time"
)

//Job represents job history
type Job struct {
	ID string `json:",omitempty"`
	TimeTakenInMs int
	EndTime time.Time
	SourceCount int
	DestCount int
	Transferred int
	Partitions map[string]int
	Chunks map[string]int
	Status string
	Error string
}


//NewJob returns a new job
func NewJob(job *core.Job) *Job {
	result := &Job{
		ID:job.ID,
		TimeTakenInMs:int(job.EndTime.Sub(job.StartTime))/int(time.Millisecond),
		Partitions:make(map[string]int),
		Chunks:make(map[string]int),
		Status:job.Status,
		Error:job.Error,
		EndTime:*job.EndTime,
	}
	methods := result.Partitions
	if job.Chunked {
		methods = result.Chunks
	}

	if len(job.Items) > 0 {
		for _, item := range job.Items {
			result.Transferred += int(item.Transferred)
			if item.Status == nil {
				continue
			}
			result.SourceCount += item.Source.Count()
			result.DestCount += item.Dest.Count()
			methods[item.Kind()]++
		}
	}
	return result
}
