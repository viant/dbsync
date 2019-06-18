package sync

import (
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/history"
	"dbsync/sync/jobs"
	"dbsync/sync/partition"
	"dbsync/sync/scheduler"
	"dbsync/sync/shared"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

var errPreviousJobRunning = errors.New("previous sync is running")

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
	var ctx *shared.Context


	if err = request.Init(); err == nil {
		if err = request.Validate(); err == nil {
			job, err = s.getJob(request.ID())
		}
	}
	ctx = shared.NewContext(job.ID, request.Debug)
	if err != nil {
		return nil, err
	}
	syncRequest, _ := json.Marshal(request)
	ctx.Log(fmt.Sprintf("sync: %s", syncRequest))
	if request.Async {
		go func() {
			_ = s.runSyncJob(ctx, job, request, response)
		}()
	} else {
		err = s.runSyncJob(ctx, job, request, response)
	}
	return response, err
}

func (s *service) onJobDone(ctx *shared.Context, job *core.Job, response *Response, err error) {
	if job == nil {
		response.SetError(err)
		return
	}

	if response.SetError(err) {
		job.Status = shared.StatusError
		job.Error = err.Error()
	}

	data, _ := json.Marshal(job)
	ctx.Log(fmt.Sprintf("completed: %s\n", data))
	job.Done(time.Now())
	historyJob := s.history.Register(job)
	response.Transferred = historyJob.Transferred
	response.SourceCount = historyJob.SourceCount
	response.DestCount = historyJob.DestCount
	response.Status = job.Status
}


func (s *service) runSyncJob(ctx *shared.Context, job *core.Job, request *Request, response *Response) (err error) {
	defer func() {
		s.onJobDone(ctx, job, response, err)
	}()
	dbSync := request.Sync
	service := dao.New(dbSync)
	if err = service.Init(ctx); err != nil {
		return err
	}
		partitionService := partition.New(dbSync, service, shared.NewMutex(), s.jobs, s.history)
	defer func() {
		_ = partitionService.Close()
	}()
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
	if job != nil && job.IsRunning() {
		return nil, errPreviousJobRunning
	}
	job = s.jobs.Create(ID)
	job.Status = shared.StatusRunning
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
