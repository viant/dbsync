package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	transferURL       = "http://%v/v1/api/transfer"
	transferStatusURL = "http://%v/v1/api/task/"
)

//Service represents a sync service
type Service interface {
	//Sync sync source with destination
	Sync(request *Request) *Response
	//ListJobs list active jobs
	ListJobs(request *JobListRequest) *JobListResponse
	//ListScheduled list schedule
	ListScheduled(request *ScheduleListRequest) *ScheduleListResponse
	//History returns supplied job history
	History(request *HistoryRequest) *HistoryResponse
}

type service struct {
	*Config
	*Jobs
	*StatRegistry
	scheduler *Scheduler
	mutex     *sync.Mutex
}

func (s *service) History(request *HistoryRequest) *HistoryResponse {
	return &HistoryResponse{
		s.StatRegistry.Get(request.ID),
	}
}

func (s *service) ListScheduled(request *ScheduleListRequest) *ScheduleListResponse {
	response := &ScheduleListResponse{}
	response.Runnables = s.scheduler.Scheduled()
	return response
}

func (s *service) ListJobs(request *JobListRequest) *JobListResponse {
	response := &JobListResponse{}
	if len(request.Ids) == 0 {
		response.Jobs = s.Jobs.List()
	} else {
		for _, id := range request.Ids {
			response.Jobs = make([]*Job, 0)
			if job := s.Jobs.Get(id); job != nil {
				response.Jobs = append(response.Jobs, job)
			}
		}
	}
	return response
}

func (s *service) Sync(request *Request) *Response {
	response := &Response{
		JobID:  request.ID(),
		Status: StatusRunning,
	}

	s.mutex.Lock()
	job := s.Jobs.Get(request.ID())
	s.mutex.Unlock()
	if job != nil && job.Status == StatusRunning {
		response.Status = StatusError
		response.Error = "previous sync is running"
		return response
	}

	if request.Async {
		go s.sync(request, response)
	} else {
		s.sync(request, response)
	}
	return response
}

func (s *service) sync(request *Request, response *Response) {
	if err := request.Init(); response.SetError(err) {
		log.Print(err)
		return
	}

	if err := request.Validate(); response.SetError(err) {
		log.Print(err)
		return
	}

	session, err := NewSession(request, response, s.Config)
	if response.SetError(err) {
		log.Print(err)
		return
	}

	log.Printf("[%v] starting sync\n", request.ID())
	defer func() {
		session.Job.Update()
		log.Printf("[%v] changed: %v, processed: %v, time taken %v ms\n", request.ID(), session.Job.Progress.SourceCount, session.Job.Progress.Transferred, int(session.Job.Elapsed/time.Millisecond))
		stats := s.StatRegistry.GetOrCreate(request.ID())
		syncStats := NewSyncStat(session.Job)
		if session.Partitions == nil {
			session.Error(nil, "partitions were nil")
			return
		}

		for _, partition := range session.Partitions.index {
			if partition.Info == nil {
				continue
			}
			method := partition.Method
			if method == "" {
				method = "inSync"
			} else {
				syncStats.PartitionTransferred++
			}
			syncStats.Methods[method]++
		}
		response.Transferred = int(session.Job.Progress.Transferred)
		stats.Add(syncStats)

	}()
	s.Add(session.Job)
	defer session.Close()

	if err = s.buildPartitions(session); session.SetError(err) {
		return
	}
	if err = s.syncDataPartitions(session); err != nil {
		session.SetError(err)
		return
	}
	session.SetStatus(StatusDone)
}

func (s *service) buildPartitions(session *Session) error {
	var partitionsRecords = make([]map[string]interface{}, 0)
	if session.Request.Partition.ProviderSQL != "" {
		session.Job.Stage = "reading partitions"
		if err := session.SourceDB.ReadAll(&partitionsRecords, session.Request.Partition.ProviderSQL, nil, nil); err != nil {
			return err
		}
	}
	if len(partitionsRecords) == 0 {
		partitionsRecords = append(partitionsRecords, map[string]interface{}{})
	}
	uniqueColumn := ""
	if len(session.Builder.IDColumns) > 0 {
		uniqueColumn = session.Builder.IDColumns[0]
	}
	var partitions = make([]*Partition, 0)
	for _, values := range partitionsRecords {
		if session.Builder.isUpperCase {
			var upperCaseValues = make(map[string]interface{})
			for k, v := range values {
				upperCaseValues[strings.ToUpper(k)] = v
			}
			values = upperCaseValues
		}
		partitions = append(partitions, NewPartition(session.Request.Partition, values, session.Request.Chunk.Threads, uniqueColumn))
	}
	session.Partitions = NewPartitions(partitions, session)
	return nil
}

func (s *service) createTransientDest(session *Session, suffix string) error {
	if suffix == "" {
		return fmt.Errorf("suffix was empty")
	}
	table := session.Builder.Table(suffix)
	_ = s.dropTable(session, table)

	dialect := dsc.GetDatastoreDialect(session.DestDB.Config().DriverName)
	dbName, _ := dialect.GetCurrentDatastore(session.DestDB)
	DDL := session.Builder.DDLFromSelect(suffix)
	_, err := session.DestDB.Execute(DDL)
	if err == nil {
		return nil
	}
	//Fallback to dialect DDL
	DDL = session.Builder.DDL(suffix)
	if session.Request.Transfer.TempDatabase != "" {
		DDL = strings.Replace(DDL, dbName+".", "", 1)
	}
	_, err = session.DestDB.Execute(DDL)
	return err
}

func (s *service) transferDataWithRetries(session *Session, transferJob *TransferJob) error {
	var err error

	for transferJob.Attempts < transferJob.MaxRetries {
		err = s.transferData(session, transferJob)
		if err == nil {
			return nil
		}
		if IsTransferError(err) {
			transferJob.Attempts++
			continue
		}
		time.Sleep(time.Second * time.Duration(transferJob.Attempts%10))
	}
	transferJob.SetError(err)
	return err
}

func (s *service) transferData(session *Session, transferJob *TransferJob) error {
	if err := s.createTransientDest(session, transferJob.Suffix); err != nil {
		return err
	}
	session.Log(nil, fmt.Sprintf("post: %v\n", transferJob.TargetURL))
	if session.IsDebug() {
		_ = toolbox.DumpIndent(transferJob.TransferRequest, true)
	}
	var response = &TransferResponse{}
	err := toolbox.RouteToService("post", transferJob.TargetURL, transferJob.TransferRequest, response)
	if err != nil {
		return err
	}
	session.Log(nil, fmt.Sprintf("response.Status: %v, %v\n", response.Status, response.Error))
	if response.Status == StatusError {
		return NewTransferError(response)
	}
	if response.Status == StatusDone {
		if response != nil && response.WriteCount > 0 {
			transferJob.SetTransferredCount(response.WriteCount)
		}
		return nil
	}
	return waitForSync(response.TaskID, transferJob)
}

func (s *service) transferDataChunks(session *Session, partition *Partition) {
	index := 0
	for !session.IsClosed() {
		if index >= partition.ChunkSize() {
			time.Sleep(time.Second)
			if index >= partition.ChunkSize() && partition.IsDone() {
				break
			}
			continue
		}
		if session.IsClosed() {
			return
		}
		partition.channel <- true
		go s.transferDataChunk(session, partition, index)
		index++
	}
}

func (s *service) transferDataChunk(session *Session, partition *Partition, index int) {
	defer partition.WaitGroup.Done()
	defer func() { <-partition.channel }()
	chunk := partition.Chunk(index)

	transferJob := session.buildTransferJob(partition, chunk.Criteria, chunk.Suffix, chunk.Count(), 0)
	session.Job.Add(transferJob)
	err := s.transferDataWithRetries(session, transferJob)
	chunk.Transfer = transferJob
	chunk.Status = StatusOk

	if err == nil {
		if chunk.Method == SyncMethodDeleteMerge {
			err = s.removeInconsistency(session, chunk, partition)
		}
		if err == nil {
			if !session.isBatchedChunk {
				err = s.mergeData(session, chunk.Suffix, chunk.Criteria)
			} else {
				err = s.appendData(session, chunk.Suffix, partition.Suffix)
			}
		}
	}
	if err != nil {
		chunk.Status = StatusError
		return
	}
}

func (s *service) removeInconsistency(session *Session, chunk *Chunk, partition *Partition) error {
	return s.deleteData(session, chunk.Suffix, chunk.Criteria)
}

//appendNewRecords removed all record from transient table that exist in dest, then appends only new
func (s *service) appendNewRecords(session *Session, suffix string, criteria map[string]interface{}) error {
	DML := session.Builder.transientDeleteDML(suffix, criteria)
	session.Log(nil, fmt.Sprintf("DML(transient):\n\t%v", DML))
	if _, err := session.DestDB.Execute(DML); err != nil {
		return err
	}
	return s.appendData(session, suffix, "")
}

func (s *service) mergeData(session *Session, suffix string, criteria map[string]interface{}) error {
	if session.Request.AppendOnly {
		return s.appendNewRecords(session, suffix, criteria)
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	DML, err := session.Builder.DML(session.Request.MergeStyle, suffix, criteria)
	session.Log(nil, fmt.Sprintf("DML(merge):\n\t%v", DML))
	if _, err = session.DestDB.Execute(DML); err == nil {
		err = s.dropTable(session, session.Builder.Table(suffix))
	}
	return err
}

func (s *service) mergePartitionData(session *Session, partition *Partition) error {
	session.Partitions.Lock()
	defer session.Partitions.Unlock()
	return s.mergeData(session, partition.Suffix, partition.criteria)
}

func (s *service) appendData(session *Session, sourceSuffix, destSuffix string) error {
	DML := session.Builder.AppendDML(sourceSuffix, destSuffix)
	session.Log(nil, fmt.Sprintf("DML(append):\n\t%v", DML))
	var err error
	if _, err = session.DestDB.Execute(DML); err == nil {
		err = s.dropTable(session, session.Builder.Table(sourceSuffix))
	}
	return err
}

func (s *service) dropTable(session *Session, table string) error {
	dialect := dsc.GetDatastoreDialect(session.DestDB.Config().DriverName)
	dbName, _ := dialect.GetCurrentDatastore(session.DestDB)

	if session.Request.Transfer.TempDatabase != "" {
		dbName = session.Request.Transfer.TempDatabase
	}
	return dialect.DropTable(session.DestDB, dbName, table)
}

func (s *service) deletePartitionData(session *Session, partition *Partition) error {
	return s.deleteData(session, partition.Suffix, partition.criteria)
}

func (s *service) deleteData(session *Session, suffix string, criteria map[string]interface{}) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	DML, err := session.Builder.DML(DMLDelete, suffix, criteria)
	session.Log(nil, fmt.Sprintf("DML:\n\t%v", DML))
	if err != nil {
		return err
	}
	_, err = session.DestDB.Execute(DML)
	return err
}

func (s *service) getPartitionSyncInfo(session *Session, partition *Partition, optimizeSync bool) (*Info, error) {
	if partition.Info != nil {
		return partition.Info, nil
	}

	if optimizeSync {
		info, err := session.GetSyncInfo(partition.criteria, true)
		if err != nil {
			return nil, err
		}
		partition.SetInfo(info)
	}
	if partition.Info == nil {
		partition.Info = &Info{Method: SyncMethodMerge}
		if !optimizeSync {
			partition.Info.Method = SyncMethodInsert
		}
	}
	return partition.Info, nil
}

func (s *service) syncDataPartitions(session *Session) error {
	optimizeSync := !session.Request.Force

	if optimizeSync {
		session.Job.Stage = "batching partitions sync status"
		if err := session.BatchSyncInfo(); err != nil {
			return err
		}
	}
	session.Job.Stage = "processing partition"

	if optimizeSync && session.isPartitioned && session.isBatchedPartition {
		if err := session.Partitions.Range(func(partition *Partition) error {
			_, err := s.getPartitionSyncInfo(session, partition, optimizeSync)
			return err
		}); err != nil {
			return err
		}
		updateBatchedPartitions(session)
	}

	err := session.Partitions.Range(func(partition *Partition) error {
		info, err := s.getPartitionSyncInfo(session, partition, optimizeSync)
		if err != nil {
			return err
		}
		if optimizeSync {
			if info.InSync {
				return nil
			}
			if info.SyncFromID > 0 {
				partition.criteria[session.Builder.IDColumns[0]] = &greaterThan{value: info.SyncFromID}
			}
		}
		session.SetSynMethod(partition.Method)
		if session.isChunkedTransfer {
			err = s.syncDataPartitionWithChunks(session, partition)
		} else {
			err = s.syncDataPartition(session, partition)
		}
		if err == nil {
			err = s.syncTransferred(session, partition)
		}
		return err
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *service) syncTransferred(session *Session, partition *Partition) error {
	var err error
	session.Log(partition, fmt.Sprintf("sync method: %v", partition.Method))
	if !session.isBatchedChunk {
		return nil
	}
	switch partition.Method {
	case SyncMethodDeleteInsert:
		if err = s.deletePartitionData(session, partition); err != nil {
			return err
		}
		fallthrough
	case SyncMethodInsert:
		err = s.appendData(session, partition.Suffix, "")
		break
	case SyncMethodDeleteMerge:
		if err = s.deletePartitionData(session, partition); err != nil {
			return err
		}
		fallthrough
	default:
		err = s.mergePartitionData(session, partition)
	}
	return err
}

func (s *service) syncDataPartition(session *Session, partition *Partition) error {
	if err := s.createTransientDest(session, partition.Suffix); err != nil {
		return err
	}
	transferJob := session.buildTransferJob(partition, partition.criteria, partition.Suffix, partition.SourceCount, partition.DestCount)
	session.Job.Add(transferJob)
	return s.transferDataWithRetries(session, transferJob)
}

func (s *service) syncDataPartitionWithChunks(session *Session, partition *Partition) error {
	var err error
	if err = s.createTransientDest(session, partition.Suffix); err != nil {
		return err
	}
	go s.transferDataChunks(session, partition)
	chunker := newChunker(session, partition)
	if err = chunker.build(session, partition); err != nil {
		return err
	}
	partition.SetDone(1)
	if partition.ChunkSize() > 0 {
		partition.WaitGroup.Wait()
	}
	return nil
}

func newChunk(sourceInfo *ChunkInfo, destInfo *ChunkInfo, min, max int, partition *Partition, criteria map[string]interface{}) *Chunk {
	chunk := &Chunk{ChunkInfo: *sourceInfo}

	if destInfo.Count() == 0 {
		chunk.SetSyncMethod(SyncMethodInsert)
		partition.SetSynMethod(SyncMethodInsert)
	} else if destInfo.Count() >= sourceInfo.Count() ||
		destInfo.Max() > sourceInfo.Max() ||
		destInfo.Min() < sourceInfo.Min() {
		chunk.SetSyncMethod(SyncMethodDeleteMerge)
		partition.SetSynMethod(SyncMethodMerge)
	} else {
		chunk.SetSyncMethod(SyncMethodMerge)
		partition.SetSynMethod(SyncMethodMerge)
	}
	chunk.MinValue = min
	chunk.MaxValue = max
	chunk.Criteria = criteria
	return chunk
}

//New creates a new service or error
func New(config *Config) (Service, error) {
	service := &service{
		Config:       config,
		Jobs:         NewJobs(),
		StatRegistry: NewStatRegistry(config.MaxHistory),
		mutex:        &sync.Mutex{},
	}
	var err error
	service.scheduler, err = NewScheduler(service, config)
	return service, err
}
