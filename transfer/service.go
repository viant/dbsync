package transfer

import (
	"fmt"
	"github.com/viant/dsc"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxTaskHistory  = 10

//Service represents a transfer service
type Service struct {
	mux      *sync.RWMutex
	tasks    map[int]*Task
	callback func(task *Task)
}

//Tasks returns a tasks
func (s *Service) Tasks() *TasksResponse {
	var response = &TasksResponse{
		Tasks: make([]*Task, 0),
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	var taskCount = len(s.tasks)
	for k, task := range s.tasks {
		if taskCount > maxTaskHistory && task.CanEvict() {
			delete(s.tasks, k)
		}
		response.Tasks = append(response.Tasks, task)
	}
	sort.Sort(response.Tasks)
	return response
}

//Task returns a task for ID
func (s *Service) Task(id int, writer http.ResponseWriter) *Task {
	s.mux.RLock()
	defer s.mux.RUnlock()
	response, ok := s.tasks[id]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return nil
	}
	return response
}

//Transfer transfer data
func (s *Service) Transfer(request *Request) *Response {
	var response = &Response{Status: StatusOk}
	rand.Seed((time.Now().UTC().UnixNano()))
	response.TaskID = int(rand.Int31())
	var task *Task
	var err error
	if err = request.Init(); err == nil {
		if err = request.Validate(); err == nil {
			task, err = NewTask(request)
		}
	}
	if err != nil {
		response.SetError(err)
		return response
	}
	s.mux.Lock()
	task.ID = response.TaskID
	s.tasks[task.ID] = task
	s.mux.Unlock()
	task.Request = request
	if !request.Async {
		s.transfer(request, response, task)
	} else {
		go s.transfer(request, response, task)
	}
	return response
}

func (s *Service) transfer(request *Request, response *Response, task *Task) {
	var err error
	defer func() {
		var endTime = time.Now()
		task.EndTime = &endTime
		task.TimeTakenMs = int(task.EndTime.Sub(task.StartTime) / time.Millisecond)
		if response.Error == "" {
			task.Status = StatusDone
			response.Status = StatusDone
			response.WriteCount = int(task.WriteCount)
		} else {
			task.Status = StatusError
			response.Status = StatusError
		}
		_ = task.dest.ConnectionProvider().Close()
		_ = task.source.ConnectionProvider().Close()
	}()
	if err != nil {
		response.SetError(err)
	}
	for i := 0; i < request.WriterThreads; i++ {
		go s.writeData(request, response, task, task.transfers.transfers[i])
	}
	err = s.readData(request, response, task)
	response.SetError(err)
	task.isWriteCompleted.Wait()
	if s.callback != nil {
		s.callback(task)
	}
}

func (s *Service) getTargetTable(request *Request, task *Task, batch *transferBatch) (*dsc.TableDescriptor, error) {
	table := task.dest.TableDescriptorRegistry().Get(request.Dest.Table)
	if table == nil {
		return nil, fmt.Errorf("target table %v not found", request.Dest.Table)
	}
	if len(table.Columns) == 0 && batch.size > 0 {
		table.Columns = []string{}
		for _, field := range batch.fields {
			table.Columns = append(table.Columns, field.Name)
		}
	}
	return table, nil
}

func (s *Service) writeData(request *Request, response *Response, task *Task, transfer *transfer) {
	var err error
	task.isWriteCompleted.Add(1)
	var count = 0
	defer func() {
		task.isWriteCompleted.Done()
		if err != nil {
			task.SetError(err)
			response.SetError(err)
			transfer.close()
		}
	}()
	var persist func(batch *transferBatch) error
	batch := transfer.getBatch()
	var table *dsc.TableDescriptor
	table, err = s.getTargetTable(request, task, batch)
	if err != nil {
		return
	}
	dmlProvider := dsc.NewMapDmlProvider(table)
	sqlProvider := func(item interface{}) *dsc.ParametrizedSQL {
		return dmlProvider.Get(dsc.SQLTypeInsert, item)
	}
	connection, err := task.dest.ConnectionProvider().Get()
	if err != nil {
		return
	}
	defer func() {
		_ = connection.Close()
	}()

	persist = func(batch *transferBatch) error {
		if batch.size == 0 {
			return nil
		}
		_, err = task.dest.PersistData(connection, batch.ranger, request.Dest.Table, dmlProvider, sqlProvider)
		if err == nil {
			atomic.AddUint64(&task.WriteCount, uint64(batch.size))
		}
		count += batch.size
		return err
	}
	if err = persist(batch); err != nil {
		return
	}
	for {
		if task.HasError() {
			break
		}
		batch := transfer.getBatch()
		if batch.size == 0 && !task.IsReading() {
			break
		}
		if err = persist(batch); err != nil {
			return
		}
	}
	err = persist(transfer.getBatch())
}

func (s *Service) readData(request *Request, response *Response, task *Task) error {
	atomic.StoreInt32(&task.isReadCompleted, 0)
	var err error
	defer func() {
		atomic.StoreInt32(&task.isReadCompleted, 1)
		if err != nil {
			task.SetError(err)
			response.SetError(err)
		}
		for _, transfer := range task.transfers.transfers {
			transfer.close()
		}
	}()

	err = task.source.ReadAllWithHandler(request.Source.Query, nil, func(scanner dsc.Scanner) (bool, error) {
		if task.HasError() {
			return false, nil
		}
		var record = make(map[string]interface{})
		task.ReadCount++

		err := scanner.Scan(&record)
		if err != nil {
			return false, fmt.Errorf("failed to scan:%v", err)
		}
		err = task.transfers.push(record)
		return err == nil, err
	})

	return err
}

//New creates a new transfer service
func New(callback func(task *Task)) *Service {
	return &Service{
		callback: callback,
		mux:      &sync.RWMutex{},
		tasks:    make(map[int]*Task),
	}
}
