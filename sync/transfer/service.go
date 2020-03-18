package transfer

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"github.com/go-errors/errors"

	"dbsync/sync/shared"
	"dbsync/sync/sql"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
	"sync/atomic"
	"time"
)

const (
	transferURL       = "http://%v/v1/api/transfer"
	transferStatusURL = "http://%v/v1/api/task/"
	defaultRetries    = 3
	baseSleep         = 15 * time.Second
)

//Service represents transfer service
type Service interface {
	//NewRequest creates a new transfer request
	NewRequest(ctx *shared.Context, transferable *core.Transferable) *Request
	//Post submit transfer request
	Post(ctx *shared.Context, request *Request, transferable *core.Transferable) error
}

type service struct {
	*contract.Sync
	dao dao.Service
	*sql.Builder
}

func (s *service) destConfig(ctx *shared.Context) *dsc.Config {
	result := s.Dest.Config.Clone()
	if s.Transfer.TempDatabase == "" {
		return result
	}
	result.Parameters = make(map[string]interface{})
	dbName, _ := s.dao.DbName(ctx, contract.ResourceKindDest)
	if s.Transfer.DriverName != "" {
		result.DriverName = s.Transfer.DriverName
	}
	for k, v := range s.Dest.Config.Parameters {
		result.Parameters[k] = v
		if textValue, ok := v.(string); ok {
			result.Parameters[k] = strings.Replace(textValue, dbName, s.Transfer.TempDatabase, 1)
		}
	}
	if s.Transfer.Descriptor != "" {
		result.Descriptor = s.Transfer.Descriptor
	}
	result.Descriptor = strings.Replace(result.Descriptor, dbName, s.Transfer.TempDatabase, 1)
	return result
}

//NewRequest returns new transfer request
func (s *service) NewRequest(ctx *shared.Context, transferable *core.Transferable) *Request {
	DQL := s.Builder.DQL("", s.Source, transferable.Filter, false)
	suffix := transferable.Suffix
	if transferable.IsDirect {
		suffix = ""
	}
	destTable := s.Builder.Table(suffix)
	if !transferable.IsDirect && s.Transfer.TempDatabase != "" {
		destTable = strings.Replace(destTable, s.Transfer.TempDatabase+".", "", 1)
	}
	return &Request{
		Source: &Source{
			Config: s.Source.Config.Clone(),
			Query:  DQL,
		},
		Dest: &Dest{
			Table:  destTable,
			Config: s.destConfig(ctx),
		},
		Async:       s.Async,
		WriterCount: s.Transfer.WriterThreads,
		BatchSize:   s.Transfer.BatchSize,
		Mode:        shared.SyncMethodInsert,
	}
}

func (s *service) waitForSync(ctx *shared.Context, syncTaskID int, transferable *core.Transferable, seq int) error {
	statusURL := fmt.Sprintf(transferStatusURL, s.Transfer.EndpointIP)
	URL := statusURL + fmt.Sprintf("%d", syncTaskID)
	response := &Response{Status: shared.StatusUnknown}
	var err error

	checkRetry := 0
	for i := 0; ; i++ {
		err = toolbox.RouteToService("get", URL, nil, response)
		ctx.Log(fmt.Sprintf("checking status: %v %+v\n", URL, response))
		if err != nil {
			if checkRetry < defaultRetries {
				time.Sleep(baseSleep * time.Duration(1+checkRetry))
				checkRetry++
				continue
			}
		}
		if response.Status == shared.StatusUnknown {
			return errors.Errorf("unknown job status: %v", URL)
		}
		if response.WriteCount > 0 {
			transferable.SetTransferred(response.WriteCount)
		}
		if err != nil || response.Status != shared.StatusRunning {
			break
		}
		if i == 0 {
			time.Sleep(3 * time.Second)
			continue
		}
		time.Sleep(15 * time.Second)
	}

	if response.Status == "error" {
		return NewError(response)
	}
	return err
}

var seq = uint32(0)

//Post post transfer job
func (s *service) Post(ctx *shared.Context, request *Request, transferable *core.Transferable) (err error) {
	transferable.DQL = request.Source.Query
	if !transferable.IsDirect {
		if err = s.dao.RecreateTransientTable(ctx, transferable.Suffix); err != nil {
			return err
		}
	}
	maxRetries := s.Transfer.MaxRetries
	if maxRetries == 0 {
		maxRetries = defaultRetries
	}
	atomic.StoreUint32(&transferable.Transferred, 0)
	attempt := 0

	lseq := atomic.AddUint32(&seq, 1)
	defer func() {
		ctx.Log(fmt.Sprintf("completed transfer %v, %v\n", lseq, err))
	}()
	for i := 0; attempt < maxRetries; i++ {
		ctx.Log(fmt.Sprintf("run transfer %v ... \n", lseq))
		err = s.post(ctx, request, transferable, lseq)
		if err == nil {
			break
		}
		if IsTransferError(err) {
			attempt++
			continue
		}
		time.Sleep(time.Second * (1 + time.Duration(i%10)))
	}
	return err
}

func (s *service) post(ctx *shared.Context, request *Request, transferable *core.Transferable, seq uint32) (err error) {
	targetURL := fmt.Sprintf(transferURL, s.Transfer.EndpointIP)
	ctx.Log(fmt.Sprintf("post: %v\n", targetURL))
	if ctx.Debug {
		_ = toolbox.DumpIndent(request, true)
	}
	var response = &Response{}
	if err = toolbox.RouteToService("post", targetURL, request, response); err != nil {
		return err
	}
	ctx.Log(fmt.Sprintf("response.Status: %v, %v\n", response.Status, response.Error))
	if response.Status == shared.StatusError {
		return NewError(response)
	}
	if response.WriteCount > 0 {
		transferable.SetTransferred(response.WriteCount)
	}
	if response.Status == shared.StatusDone {
		return nil
	}
	return s.waitForSync(ctx, response.TaskID, transferable, int(seq))
}

func newService(sync *contract.Sync, dao dao.Service) *service {
	return &service{
		Sync:    sync,
		dao:     dao,
		Builder: dao.Builder(),
	}
}

//New creates a transfer service
func New(sync *contract.Sync, dao dao.Service) Service {
	return newService(sync, dao)
}
