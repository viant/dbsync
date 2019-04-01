package sync

import (
	"fmt"
	"github.com/viant/dsc"
)

//Transfer represents transferData config
type Transfer struct {
	BatchSize     int
	WriterThreads int
	EndpointIP    string
	MaxRetries    int
}

type Source struct {
	*dsc.Config
	Query string
}

type Dest struct {
	*dsc.Config
	Table string
}

type TransferRequest struct {
	Source      *Source
	Dest        *Dest
	WriterCount int
	BatchSize   int
	Mode        string
	OmitEmpty   bool
}

type TransferResponse struct {
	TaskId int
	Status string
	Error  string
}

type TransferJob struct {
	TransferRequest *TransferRequest
	Suffix          string
	TargetURL       string
	StatusURL       string
	MaxRetries      int
	Attempts        int
	err             error
}

type TransferError struct {
	*TransferResponse
}

func (r *TransferError) Error() string {
	return fmt.Sprintf("%v", r.TransferResponse.Error)
}

//NewTransferError creates a new transferData error
func NewTransferError(response *TransferResponse) *TransferError {
	return &TransferError{
		TransferResponse: response,
	}
}

//IsTransferError returns true if error is TransferError type
func IsTransferError(err error) bool {
	_, ok := err.(*TransferError)
	return ok
}
