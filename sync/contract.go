package sync

import (
	"dbsync/sync/model"
	"fmt"
	"github.com/viant/toolbox/url"
	"strings"
)

const (
	//StatusOk ok status
	StatusOk = "ok"
	//StatusError error status
	StatusError = "error"
	//StatusDone done status
	StatusDone = "done"
	//StatusRunning sync1 running status
	StatusRunning = "running"
)

//Request represnet sync1 request
type Request struct {
	Id string
	*model.Sync
	Schedule *model.Schedule
}

//Response return response
type Response struct {
	JobID       string `json:",ommitempty"`
	Status      string
	Transferred int
	Error       string `json:",ommitempty"`
}

//JobListRequest represents a list request
type JobListRequest struct {
	Ids []string //if empty return all job
}

//JobListResponse reprsents a list response
type JobListResponse struct {
	Jobs []*Job
}

//ScheduleListRequest represents a list request
type ScheduleListRequest struct {
}

//ScheduleListResponse represents a list response
type ScheduleListResponse struct {
	Runnables []ScheduleRunnable
}

//HistoryRequest represent history request
type HistoryRequest struct {
	ID string
}

//HistoryResponse represent history response
type HistoryResponse struct {
	*Stats
}

//NewRequestFromURL create a new request from URL
func NewRequestFromURL(URL string) (*Request, error) {
	resource := url.NewResource(URL)
	result := &Request{}
	return result, resource.Decode(result)
}

//ID returns sync1 request ID
func (r *Request) ID() string {
	if r.Id != "" {
		return r.Id
	}
	_ = r.Dest.Config.Init()
	if r.Dest.Config.Has("dbname") {
		return r.Dest.Config.Get("dbname") + ":" + r.Dest.Table
	}
	return r.Dest.Table
}

//SetError sets error message and returns true if err was not nil
func (r *Response) SetError(err error) bool {
	if err == nil {
		return false
	}
	r.Status = StatusError
	r.Error = err.Error()
	return true
}


//ScheduledRun returns schedule details and run function
func (r *Request) ScheduledRun() (*model.Schedule, func(service Service) error) {
	return r.Schedule, func(service Service) error {
		response := service.Sync(r)
		if response.Error != "" {
			return fmt.Errorf(response.Error)
		}
		return nil
	}
}

//UseUpperCaseSQL update id, partition column to upper case
func (r *Request) UseUpperCase() {
	if len(r.IDColumns) > 0 {
		for i, v := range r.IDColumns {
			r.IDColumns[i] = strings.ToUpper(v)
		}
	}
	if len(r.Partition.Columns) > 0 {
		for i, v := range r.Partition.Columns {
			r.Partition.Columns[i] = strings.ToUpper(v)
		}
	}

}

//NewSyncRequestFromURL creates a new resource from URL
func NewSyncRequestFromURL(URL string) (*Request, error) {
	request := &Request{}
	resource := url.NewResource(URL)
	return request, resource.Decode(request)
}
