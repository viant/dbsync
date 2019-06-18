package jobs

import "dbsync/sync/core"

//Service represents a job service
type Service interface {
	List(request *ListRequest) *ListResponse
	Create(ID string) *core.Job
	Get(ID string) *core.Job
}

type service struct {
	registry *registry
}

//Get returns job by ID or nil
func (s *service) Get(ID string) *core.Job {
	jobs := s.registry.list()
	for  i := range jobs {
		if jobs[i].ID == ID {
			jobs[i].Update()
			return jobs[i]
		}
	}
	return nil

}

//List lists all jobs
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
		jobs[i].Update()
		filtered = append(filtered, jobs[i])
	}
	return &ListResponse{
		Jobs: filtered,
	}
}

//Create creates a new job
func (s *service) Create(ID string) *core.Job {
	job := core.NewJob(ID)
	s.registry.add(job)
	return job
}


//New create a job service
func New() Service {
	return &service{
		registry: newRegistry(),
	}
}
