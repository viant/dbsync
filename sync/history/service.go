package history

import (
	"dbsync/sync/core"
	"dbsync/sync/shared"
	"fmt"
	"time"
)

//Service reprsents history service
type Service interface {
	//Register add job to history
	Register(job *core.Job) *Job
	//Show shows history
	Show(request *ShowRequest) *ShowResponse
	//Status returns status for past job
	Status(request *StatusRequest) *StatusResponse
}

type service struct {
	startTime time.Time
	registry  *registry
}

//Status returns status
func (s *service) Status(request *StatusRequest) *StatusResponse {
	jobs := s.registry.list(request.RunCount)
	response := NewStatusResponse()
	if len(jobs) == 0 {
		return response
	}

	for k := range jobs {
		history := jobs[k]
		for _, item := range history {
			if item.Status == shared.StatusError {
				response.Error = item.Error
				response.Status.Status = item.Status
				response.Errors[item.ID] = item.Error
				continue
			}
			if response.LastSyncTime == nil {
				response.LastSyncTime = &item.EndTime
			}
			if response.LastSyncTime.Before(item.EndTime) {
				response.LastSyncTime = &item.EndTime
			}
		}
		if history[0].Status == shared.StatusDone {
			response.Transferred[k] = history[0].Transferred
		}

	}
	response.UpTime = fmt.Sprintf("%s", time.Now().Sub(s.startTime))
	return response
}

//Show show status
func (s *service) Show(request *ShowRequest) *ShowResponse {
	return &ShowResponse{Items: s.registry.get(request.ID)}
}

//Register register a job
func (s *service) Register(coreJob *core.Job) *Job {
	job := NewJob(coreJob)
	s.registry.register(job)
	return job
}

//New creates a new history service
func New(config *shared.Config) Service {
	return &service{
		startTime: time.Now(),
		registry:  newRegistry(config.MaxHistory),
	}
}
