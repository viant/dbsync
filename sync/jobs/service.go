package jobs

import "dbsync/sync/core"

type Service interface {
	List(request *ListRequest) *ListResponse
	Create(ID string) *core.Job
	Get(ID string) *core.Job
}

type service struct {
	registry *registry
}


func (s *service) Get(ID string) *core.Job {
	jobs := s.registry.list()
	for  i := range jobs {
		if jobs[i].ID == ID {
			return jobs[i]
		}
	}
	return s.Create(ID)

}

func (s *service) List(request *ListRequest) *ListResponse {
	jobs := s.registry.list()
	if len(request.IDs) == 0 {
		return &ListResponse{
			Jobs: jobs,
		}
	}
	var requestedIDs = make(map[string]bool)
	for i := range request.IDs {
		requestedIDs[request.IDs[i]] = true
	}
	var filtered = make([]*core.Job, 0)
	for i := range jobs {
		if _, has := requestedIDs[jobs[i].ID]; !has {
			continue
		}
		filtered = append(filtered, jobs[i])
	}
	return &ListResponse{
		Jobs: filtered,
	}
}

func (s *service) Create(ID string) *core.Job {
	job := core.NewJob(ID)
	s.registry.add(job)
	return job
}

func New() *service {
	return &service{
		registry: newRegistry(),
	}
}
