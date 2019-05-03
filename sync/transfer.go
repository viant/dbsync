package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"sort"
	"sync"
	"time"
)

const transientTableSuffix = "_tmp"

//Transfer represents transferData config
type Transfer struct {
	BatchSize     int
	WriterThreads int
	EndpointIP    string
	MaxRetries    int
	TempDatabase  string
	Suffix        string
}

//Source source
type Source struct {
	*dsc.Config
	Query string
}

//Dest dest
type Dest struct {
	*dsc.Config
	Table string
}

//TransferRequest represents transfer service request
type TransferRequest struct {
	Source      *Source
	Dest        *Dest
	WriterCount int
	Async       bool
	BatchSize   int
	Mode        string
	OmitEmpty   bool
}

//TransferResponse represents transfer service response
type TransferResponse struct {
	TaskID     int
	Status     string
	Error      string
	WriteCount int
}

//TransferError represents transfer error
type TransferError struct {
	*TransferResponse
}

//Error error interface
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

//TransferJob represents transfer job
type TransferJob struct {
	Id     int
	Suffix string
	Progress
	StartTime       time.Time
	IsInSync        bool
	TransferRequest *TransferRequest
	TargetURL       string
	StatusURL       string
	MaxRetries      int
	Attempts        int
	Error           string
	TimeTaken       time.Duration
	EndTime         *time.Time
	err             error
}

//Transfers represents transfers
type Transfers struct {
	Transfers []*TransferJob
	mutex     *sync.RWMutex
}

func (t *Transfer) Init() error {
	if t.MaxRetries == 0 {
		t.MaxRetries = 2
	}
	return nil
}

//SetError sets an error
func (j *TransferJob) SetError(err error) {
	if err == nil {
		return
	}
	j.err = err
	j.Error = err.Error()
}

//Add adds transfer job
func (t *Transfers) Add(job *TransferJob) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	sort.Sort(t)
	t.Transfers = append(t.Transfers, job)
}

//Len implements sort Len interface
func (t Transfers) Len() int { return len(t.Transfers) }

//Swap implements sort Swap interface
func (t Transfers) Swap(i, j int) { t.Transfers[i], t.Transfers[j] = t.Transfers[j], t.Transfers[i] }

//Less implements sort Less interface
func (t Transfers) Less(i, j int) bool {
	return t.Transfers[i].StartTime.Before(t.Transfers[j].StartTime)
}

//NewTransfers returns new transfers
func NewTransfers() *Transfers {
	return &Transfers{
		mutex:     &sync.RWMutex{},
		Transfers: make([]*TransferJob, 0),
	}
}
