package transfer

import (
	"github.com/viant/dsc"
	"sync"
	"sync/atomic"
	"time"
)

//Task represents a transfer tasks
type Task struct {
	source           dsc.Manager
	dest             dsc.Manager
	transfers        *transfers
	isReadCompleted  int32
	isWriteCompleted *sync.WaitGroup
	hasError         int32
	ID               int
	Request          *Request
	StartTime        time.Time
	EndTime          *time.Time
	Error            string
	Status           string

	ReadCount   int
	WriteCount  uint64
	TimeTakenMs int
}

//IsReading returns true if transfer read data from the source
func (t *Task) IsReading() bool {
	return atomic.LoadInt32(&t.isReadCompleted) == 0
}

//CanEvict returns true if can evict
func (t *Task) CanEvict() bool {
	if t.EndTime == nil {
		return false
	}
	return time.Now().Sub(*t.EndTime) > time.Minute
}

//HasError returns true if error occurred
func (t *Task) HasError() bool {
	return atomic.LoadInt32(&t.hasError) == 1
}

//SetError sets error
func (t *Task) SetError(err error) {
	if err == nil {
		return
	}
	atomic.StoreInt32(&t.hasError, 1)
	t.Error = err.Error()
	t.Status = "error"
	t.transfers.close()
}

//NewTask returns a new transfer task
func NewTask(request *Request) (*Task, error) {
	var task = &Task{
		transfers:        newTransfers(request),
		isWriteCompleted: &sync.WaitGroup{},
		StartTime:        time.Now(),
		Status:           StatusRunning,
	}
	var err error
	if task.source, err = dsc.NewManagerFactory().Create(request.Source.Config); err != nil {
		return nil, err
	}
	if task.dest, err = dsc.NewManagerFactory().Create(request.Dest.Config); err != nil {
		return nil, err
	}
	return task, nil
}

//Tasks retpresents tasks
type Tasks []*Task

func (a Tasks) Len() int {
	return len(a)
}
func (a Tasks) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a Tasks) Less(i, j int) bool {
	return a[i].StartTime.After(a[j].StartTime)
}
