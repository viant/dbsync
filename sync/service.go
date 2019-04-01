package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"sync/atomic"
	"time"
)

const (
	transferURL       = "http://%v/v1/api/transferData"
	transferStatusURL = "http://%v/v1/api/task/"
)

type Service interface {
	Sync(request *SyncRequest) *SyncResponse
}

type service struct{}

func (s *service) Sync(request *SyncRequest) *SyncResponse {
	response := &SyncResponse{}
	if err := request.Init(); response.SetError(err) {
		return response
	}
	if err := request.Validate(); response.SetError(err) {
		return response
	}
	session, err := NewSession(request, response)
	if response.SetError(err) {
		return response
	}
	if request.Partition.ProviderSQL != "" {
		if err = session.SourceDB.ReadAll(&session.Partitions, request.Partition.ProviderSQL, nil, nil); session.SetError(err) {
			return response
		}
	}
	if len(session.Partitions) == 0 {
		session.Partitions = []map[string]interface{}{
			{},
		}
	}

	if isChunkTransfer := request.Sync.ChunkRowCount > 0; isChunkTransfer {
		err = s.syncDataWithChunks(session, request)
	} else {
		err = s.syncData(session, request)
	}
	response.SetError(err)
	return response
}

func (s *service) waitForSync(syncTaskId int, syncURL string) error {
	var response = &TransferResponse{}
	for ; ; {
		URL := syncURL + fmt.Sprintf("%d", syncTaskId)
		err := toolbox.RouteToService("get", URL, nil, response)
		if err != nil || response.Status != "running" {
			break;
		}
		time.Sleep(10 * time.Second)
	}
	if response.Status == "error" {
		return NewTransferError(response)
	}
	return nil
}

func (s *service) createTransientDest(session *Session, suffix string) error {
	DDL, err := session.Builder.DDL(suffix)
	if err == nil {
		_, err = session.DestDB.Execute(DDL, nil)
	}
	return err
}

func (s *service) buildTransferJob(session *Session, request *SyncRequest, criteria map[string]interface{}, suffix string) *TransferJob {
	DQL := session.Builder.DQL("", session.Source, criteria)
	transferRequest := &TransferRequest{
		Source: &Source{
			Config: session.Source.Config,
			Query:  DQL,
		},
		Dest: &Dest{
			Table:  session.Dest.Table + suffix,
			Config: session.Dest.Config,
		},

		WriterCount: request.WriterThreads,
		BatchSize:   request.BatchSize,
		Mode:        "insert",
	}
	return &TransferJob{
		MaxRetries:      request.MaxRetries,
		TransferRequest: transferRequest,
		Suffix:          suffix,
		StatusURL:       fmt.Sprintf(transferStatusURL, request.EndpointIP),
		TargetURL:       fmt.Sprintf(transferURL, request.Transfer.EndpointIP),
	}

}

func (s *service) transferDataWithRetries(session *Session, transferJob *TransferJob) error {
	defer session.syncWaitGroup.Done()
	for ; transferJob.Attempts < transferJob.MaxRetries; {
		err := s.transferData(session, transferJob)
		if err == nil {
			return nil
		}
		if IsTransferError(err) {
			transferJob.Attempts++
			continue
		}
		transferJob.err = err
		return err
	}
	return nil
}

func (s *service) transferData(session *Session, transferJob *TransferJob) error {
	if err := s.createTransientDest(session, transferJob.Suffix); err != nil {
		return err
	}
	var response = &TransferResponse{}
	if err := toolbox.RouteToService("post", transferJob.TargetURL, transferJob.TransferRequest, response); err != nil {
		return err
	}
	if response.Status == "error" {
		return NewTransferError(response)
	}
	return s.waitForSync(response.TaskId, transferJob.StatusURL)
}

func (s *service) transferDataChunks(session *Session, request *SyncRequest, done *int32) {
	index := 0
	for ; ! session.IsClosed(); {
		if index >= session.ChunkSize() {
			time.Sleep(time.Second)
			if index >= session.ChunkSize() && atomic.LoadInt32(done) == 1 {
				break
			}
			continue
		}
		if session.IsClosed() {
			return
		}
		session.chunkChannel <- true
		go s.transferDataChunk(session, request, index)
		index++
	}
}

func (s *service) transferDataChunk(session *Session, request *SyncRequest, index int) {
	session.syncWaitGroup.Add(1)
	defer func() { <-session.chunkChannel }()
	chunk := session.Chunk(index)
	transferJob := s.buildTransferJob(session, request, chunk.CriteriaValues, fmt.Sprintf("_chunk_%05d", index))
	err := s.transferDataWithRetries(session, transferJob)
	chunk.Transfer = transferJob
	chunk.Status = ChunkStatusOk
	if err != nil {
		chunk.Status = ChunkStatusError
	}
}


func (s *service) syncData(session *Session, request *SyncRequest) error {
	for _, criteriaValues := range session.Partitions {
		if err := s.createTransientDest(session, transientTableSuffix); err != nil {
			return err
		}
		transferJob := s.buildTransferJob(session, request, criteriaValues, transientTableSuffix)
		err := s.transferDataWithRetries(session, transferJob)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) syncDataWithChunks(session *Session, request *SyncRequest) error {
	offset := 0
	limit := request.Sync.ChunkRowCount
	for _, criteriaValues := range session.Partitions {
		var partitionDone = int32(0)
		if err := s.createTransientDest(session, transientTableSuffix); err != nil {
			return err
		}
		go s.transferDataChunks(session, request, &partitionDone)

		for ; ; {




			DQL, err := session.Builder.ChunkDQL(session.Source, offset, limit, criteriaValues)
			if err != nil {
				return err
			}
			chunk := &Chunk{}
			if _, err = session.SourceDB.ReadSingle(chunk, DQL, nil, nil); err != nil {
				return err
			}
			criteriaValues[session.Builder.UniqueColumns[0]] = &between{from: chunk.Min(), to: chunk.Max()}
			CountDQL := session.Builder.CountDQL("", session.Dest, criteriaValues)
			destCount := &Chunk{}
			if _, err = session.DestDB.ReadSingle(destCount, CountDQL, nil, nil); err != nil {
				return err
			}
			if chunk.Count() == 0 {
				break;
			}
			offset = chunk.Max() + 1
			//TODO use max of target to see only additions
			if destCount.Count() == chunk.Count() {
				//TODO compare data ,
				continue
			}
			if destCount.Count() == 0 {
				chunk.Method = "insert"
			} else {
				chunk.Method = "merge"
			}
			chunk.CriteriaValues = criteriaValues
			session.AddChunk(chunk)
		}

		atomic.StoreInt32(&partitionDone, 1)

		if session.ChunkSize() > 0 {
			session.syncWaitGroup.Wait()
		}

	}

	return nil
}

func New() Service {
	return &service{}
}
