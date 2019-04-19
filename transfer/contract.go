package transfer

import (
	"fmt"
	"github.com/viant/dsc"
)

const (
	//StatusOk represents ok status
	StatusOk = "ok"
	//StatusError represents error status
	StatusError = "error"
	//StatusDone represents done status
	StatusDone = "done"
	//StatusRunning represents running status
	StatusRunning = "running"
)

//Source source
type Source struct {
	*dsc.Config
	Query string
}

//Dest transfer destination
type Dest struct {
	*dsc.Config
	Table string
}

//Request represents transfer request
type Request struct {
	Source        *Source
	Dest          *Dest
	Async         bool
	BatchSize     int
	WriterThreads int `description:"number of writer go routines"`
	WriterCount   int
	OmitEmpty     bool `description:"if set set null for any 0 or empty values"`
}

//Response transfer response
type Response struct {
	TaskID     int
	WriteCount int
	Status     string
	Error      string
}

//TasksResponse tasks response
type TasksResponse struct {
	Tasks Tasks
}

//Init initializes request
func (r *Request) Init() error {
	if r.BatchSize == 0 {
		r.BatchSize = 1
	}
	if r.WriterCount != 0 {
		r.WriterThreads = r.WriterCount
	}
	return nil
}

//Validate validates request
func (r *Request) Validate() error {
	if r.Source == nil {
		return fmt.Errorf("source was empty")
	}
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if r.Dest == nil {
		return fmt.Errorf("source was empty")
	}
	return r.Dest.Validate()
}

//Validate destination
func (s *Dest) Validate() error {
	if s.Config == nil {
		return fmt.Errorf("dest config was empty")
	}
	if s.Table == "" {
		return fmt.Errorf("dest table was empty")
	}
	return nil
}

//Validate validates source
func (s *Source) Validate() error {
	if s.Config == nil {
		return fmt.Errorf("source config was empty")
	}
	if s.Query == "" {
		return fmt.Errorf("source query was empty")
	}
	return nil
}

//SetError sets error
func (r *Response) SetError(err error) {
	if err == nil {
		return
	}
	r.Status = "error"
	r.Error = err.Error()
}
