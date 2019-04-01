package transfer

import (
	"fmt"
	"github.com/viant/dsc"
)

const (
	TransferModeInsert = "insert"
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

//TransferRequest represents transfer request
type TransferRequest struct {
	Source        *Source
	Dest          *Dest
	BatchSize     int
	WriterThreads int    `description:"number of writer go routines"`
	Mode          string `description:"supported values: insert or persist"`
	OmitEmpty     bool   `description:"if set set null for any 0 or empty values"`
}


//TransferResponse transfer response
type TransferResponse struct {
	TaskId int
	Status string
	Error  string
}

//TasksResponse tasks response
type TasksResponse struct {
	Tasks Tasks
}

func (r *TransferRequest) Init() error {
	if r.BatchSize == 0 {
		r.BatchSize = 1
	}
	return nil
}

//Validate validates request
func (r *TransferRequest) Validate() error {
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

func (r *TransferResponse) SetError(err error) {
	if err == nil {
		return
	}
	r.Status = "error"
	r.Error = err.Error()
}
