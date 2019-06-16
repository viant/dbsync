package sync

import (
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/history"
	"dbsync/sync/jobs"
	"dbsync/sync/partition"
	"dbsync/sync/scheduler"
	"dbsync/sync/shared"
	"errors"
	"fmt"
	"log"
	"time"
)

var previousJobRunningErr = errors.New("previous sync1 is running")

//Service represents a sync1 service
type Service interface {
	//Sync sync1 source with destination
	Sync(request *Request) *Response
	//ListJobs list active jobs
	Scheduler() scheduler.Service
	//Jobs job service
	Jobs() jobs.Service
	//History returns history service
	History() history.Service
}

type service struct {
	*shared.Config
	jobs      jobs.Service
	history   history.Service
	scheduler scheduler.Service
	mutex     *shared.Mutex
}

func (s *service) Scheduler() scheduler.Service {
	return s.scheduler
}

func (s *service) Jobs() jobs.Service {
	return s.jobs
}

func (s *service) History() history.Service {
	return s.history
}

func (s *service) Sync(request *Request) *Response {
	response, err := s.sync(request)
	if err != nil {
		log.Printf("[%v] %v", request.ID(), err)
	}
	return response
}

func (s *service) sync(request *Request) (response *Response, err error) {
	response = &Response{
		JobID:  request.ID(),
		Status: shared.StatusRunning,
	}
	var job *core.Job
	if err = request.Init(); err == nil {
		if err = request.Validate(); err == nil {
			job, err = s.getJob(request.ID())
		}
	}
	if err != nil {
		return nil, err
	}
	if request.Async {
		go func() {
			_ = s.runSyncJob(job, request, response)
		}()
	} else {
		err = s.runSyncJob(job, request, response)
	}
	return response, err
}

func (s *service) onJobDone(job *core.Job, response *Response, err error) {
	if err != nil {
		response.SetError(err)
		if job.Status != shared.StatusError {
			job.Status = shared.StatusError
			job.Error = err.Error()
		}
	}
	now := time.Now()
	job.EndTime = &now
	historyJob := s.history.Register(job)
	response.Transferred = historyJob.Transferred
	response.SourceCount = historyJob.SourceCount
	response.DestCount = historyJob.DestCount
}


func (s *service) runSyncJob(job *core.Job, request *Request, response *Response) (err error) {
	defer func() {
		s.onJobDone(job, response, err)
	}()
	dbSync := request.Sync
	ctx := shared.NewContext(job.ID, request.Debug)
	service := dao.New(dbSync)
	if err = service.Init(ctx); err != nil {
		return err
	}
	partitionService := partition.New(dbSync, service, shared.NewMutex(), s.jobs, s.history)
	if err = partitionService.Init(ctx); err == nil {
		if err = partitionService.Build(ctx); err == nil {
			err = partitionService.Sync(ctx)
		}
	}
	return err
}

func (s *service) getJob(ID string) (*core.Job, error) {
	s.mutex.Lock(ID)
	defer s.mutex.Unlock(ID)
	job := s.jobs.Get(ID)
	if job.IsRunning() {
		return nil, previousJobRunningErr
	}
	return job, nil
}

func (s *service) runScheduledJob(schedulable *scheduler.Schedulable) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fatal error: %v", r)
		}
	}()
	_, err = s.sync(&Request{
		Id:   schedulable.ID,
		Sync: schedulable.Sync,
	})
	return err
}

//New creates a new service or error
func New(config *shared.Config) (Service, error) {
	service := &service{
		Config:  config,
		mutex:   shared.NewMutex(),
		history: history.New(config),
		jobs:    jobs.New(),
	}
	var err error
	service.scheduler, err = scheduler.New(config, service.runScheduledJob)
	return service, err
}
