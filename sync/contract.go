package sync

import (
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
	//StatusRunning sync running status
	StatusRunning = "running"
)

//Request represnet sync request
type Request struct {
	Id       string
	Strategy `yaml:",inline" json:",inline"`
	Transfer Transfer
	Dest     *Resource
	Source   *Resource
	Table    string
	Async    bool
	Debug    bool
	Criteria map[string]interface{}
	Schedule *Schedule
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

//ID returns sync request ID
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

//Init initialized Request
func (r *Request) Init() error {

	if r.Dest == nil || r.Source == nil {
		return nil
	}

	if r.MergeStyle == "" {
		if r.Dest != nil {
			switch r.Dest.DriverName {
			case "mysql":
				r.MergeStyle = DMLInsertOnDuplicateUpddate
			case "sqlite3":
				r.MergeStyle = DMLInsertOrReplace
			case "oci8", "ora":
				r.MergeStyle = DMLMergeInto
			default:
				r.MergeStyle = DMLMerge
			}
		}
	}
	if err := r.Transfer.Init(); err != nil {
		return err
	}
	if err := r.Strategy.Init(); err != nil {
		return err
	}
	if len(r.Criteria) > 0 {
		if len(r.Dest.Criteria) == 0 {
			r.Dest.Criteria = r.Criteria
		}
		if len(r.Source.Criteria) == 0 {
			r.Source.Criteria = r.Criteria
		}
	}
	if r.Diff.BatchSize > 0 && r.Partition.BatchSize == 0 {
		r.Partition.BatchSize = r.Diff.BatchSize
	}
	if r.Partition.BatchSize == 0 {
		r.Partition.BatchSize = 16
	}

	if r.Table != "" {
		if r.Dest.Table == "" {
			r.Dest.Table = r.Table
		}
		if r.Source.Table == "" {
			r.Source.Table = r.Table
		}
	}
	if r.Schedule != nil {
		if r.Schedule.At != nil {
			return r.Schedule.At.Init()
		}
	}
	return nil
}

//Validate checks if Request is valid
func (r *Request) Validate() error {
	if r.Source == nil {
		return fmt.Errorf("source was empty")
	}
	if r.Dest == nil {
		return fmt.Errorf("dest was empty")
	}
	if r.Dest.Table == "" {
		return fmt.Errorf("dest table was empty")
	}

	if r.Transfer.EndpointIP == "" {
		return fmt.Errorf("transfer.endpointIP was empty")
	}

	if r.Diff.CountOnly && len(r.Diff.Columns) > 0 {
		return fmt.Errorf("countOnly can not be set with custom columns")
	}
	if r.Chunk.Size > 0 && len(r.IDColumns) != 1 {
		return fmt.Errorf("data chunking is only supported with single unique key, chunk size: %v, unique key count: %v", r.Chunk.Size, len(r.IDColumns))
	}

	if r.Schedule != nil {
		if err := r.Schedule.Validate(); err != nil {
			return err
		}
	}
	if err := r.Source.Validate(); err != nil {
		return fmt.Errorf("source: %v", err)
	}
	if err := r.Dest.Validate(); err != nil {
		return fmt.Errorf("dest: %v", err)
	}
	return nil
}

//ScheduledRun returns schedule details and run function
func (r *Request) ScheduledRun() (*Schedule, func(service Service) error) {
	return r.Schedule, func(service Service) error {
		response := service.Sync(r)
		if response.Error != "" {
			return fmt.Errorf(response.Error)
		}
		return nil
	}
}

//UseUpperCase update id, partition column to upper case
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
