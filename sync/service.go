package sync

import (
	"fmt"
	"github.com/viant/toolbox"
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

	if isChunkTransfer := request.Sync.ChunkRowCount > 0; isChunkTransfer {
		if err := s.buildChunks(session, request); session.SetError(err) {
			return response
		}
	}
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




func (s *service) syncDataChunks(session *Session, request *SyncRequest) {
	index := 0
	for ; ! session.IsClosed(); {
		if index >= session.ChunkSize() {
			time.Sleep(time.Second)
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
	var criteria = make(map[string]interface{})
	chunk := session.Chunk(index)
	criteria[session.Builder.UniqueColumns[0]] = &between{from: chunk.Min(), to: chunk.Max()}
	transferJob := s.buildTransferJob(session, request, criteria, fmt.Sprintf("_chunk_%04d", index))
	err := s.transferDataWithRetries(session, transferJob)
	chunk.Transfer = transferJob
	chunk.Status = ChunkStatusOk
	if err != nil {
		chunk.Status = ChunkStatusError
	}
}



func (s *service) buildChunks(session *Session, request *SyncRequest) error {
	if err := s.createTransientDest(session, "_tmp"); err != nil {
		return err
	}
	offset := 0
	limit := request.Sync.ChunkRowCount
	go s.syncDataChunks(session, request)


	for ; ; {
		var values = make(map[string]interface{})
		//TODO add partition where



		DQL, err := session.Builder.ChunkDQL(session.Source, offset, limit, values)
		if err != nil {
			return err
		}
		chunk := &Chunk{}
		if _, err = session.SourceDB.ReadSingle(chunk, DQL, nil, nil); err != nil {
			return err
		}

		values[session.Builder.UniqueColumns[0]] = &between{from: chunk.Min(), to: chunk.Max()}
		CountDQL := session.Builder.CountDQL("", session.Dest, values)
		destCount := &Chunk{}
		if _, err = session.DestDB.ReadSingle(destCount, CountDQL, nil, nil); err != nil {
			return err
		}

		if chunk.Count() == 0 {
			break;
		}
		offset = chunk.Max() + 1
		if destCount.Count() == chunk.Count() {
			//TODO compare data
			continue
		}
		if destCount.Count() == 0 {
			chunk.Method = "insert"
		} else {
			chunk.Method = "merge"
		}
		session.AddChunk(chunk)
	}

	if session.ChunkSize() > 0  {
		session.syncWaitGroup.Wait()
	}

	return nil
}

func New() Service {
	return &service{}
}
