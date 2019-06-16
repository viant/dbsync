package history

import (
	"dbsync/sync/core"
	"dbsync/sync/shared"
)

type Service interface {
	Register(job *core.Job) *Job
	Show(request *ShowRequest) *ShowResponse
	Status(request *StatusRequest) *StatusResponse
}

type service struct {
	registry *registry
}

func (s *service) Status(request *StatusRequest) *StatusResponse {
	jobs := s.registry.list(request.RunCount)
	response := NewStatusResponse()
	for k := range jobs {
		history := jobs[k]
		for _, item := range history {
			if item.Status == shared.StatusError {
				response.Error = item.Error
				response.Status.Status = item.Status
				response.Errors[item.ID] = item.Error
				continue
			}
		}
		if history[0].Status == shared.StatusOk {
			response.Transferred[k] = history[0].Transferred
		}
	}
	return response
}

func (s *service) Show(request *ShowRequest) *ShowResponse {
	return &ShowResponse{Items: s.registry.get(request.ID)}
}

func (s *service) Register(coreJob *core.Job) *Job {
	job := NewJob(coreJob)
	s.registry.register(job)
}


func New(config *shared.Config) *service {
	return &service{
		registry: newRegistry(config.MaxHistory),
	}
}
