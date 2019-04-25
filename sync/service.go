package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"log"
	"strings"
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
	job := s.Jobs.Get(request.ID())
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
		return
	}
	if err := request.Validate(); response.SetError(err) {
		return
	}
	session, err := NewSession(request, response, s.Config)
	if response.SetError(err) {
		return
	}
	defer func() {
		session.Job.Update()
		log.Printf("[%v] source: %v, processed: %v, time taken %v ms\n", request.ID(), session.Job.Progress.SourceCount, session.Job.Progress.DestCount, int(session.Job.Elapsed/time.Millisecond))
		stats := s.StatRegistry.GetOrCreate(request.ID())
		syncStats := NewSyncStat(session.Job)
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
	if len(session.Builder.UniqueColumns) > 0 {
		uniqueColumn = session.Builder.UniqueColumns[0]
	}
	var partitions = make([]*Partition, 0)
	for _, values := range partitionsRecords {
		partitions = append(partitions, NewPartition(session.Request.Partition, values, session.Request.ChunkQueueSize, uniqueColumn))
	}
	session.Partitions = NewPartitions(partitions, session.Request.Partition.Threads)
	return nil
}

func (s *service) createTransientDest(session *Session, suffix string) error {
	if suffix == "" {
		return fmt.Errorf("suffix was empty")
	}
	table := session.Builder.Table(suffix)
	dialect := dsc.GetDatastoreDialect(session.DestDB.Config().DriverName)
	dbName, _ := dialect.GetCurrentDatastore(session.DestDB)
	if session.Request.TempDatabase != "" {
		_ = dialect.DropTable(session.DestDB, session.Request.TempDatabase, table)
	} else {
		_ = dialect.DropTable(session.DestDB, dbName, table)
	}
	DDL, err := session.Builder.DDL(suffix)
	if session.Request.TempDatabase != "" {
		DDL = strings.Replace(DDL, dbName+".", "", 1)
	}
	if err == nil {
		_, err = session.DestDB.Execute(DDL)
	}
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
	return nil
}

func (s *service) transferData(session *Session, transferJob *TransferJob) error {
	if err := s.createTransientDest(session, transferJob.Suffix); err != nil {
		return err
	}
	if s.Debug {
		log.Printf("post: %v\n", transferJob.TargetURL)
		_ = toolbox.DumpIndent(transferJob.TransferRequest, true)
	}
	var response = &TransferResponse{}
	err := toolbox.RouteToService("post", transferJob.TargetURL, transferJob.TransferRequest, response)
	if err != nil {
		return err
	}
	if s.Debug {
		log.Printf("response.Status: %v, %v\n", response.Status, response.Error)
	}
	if response.Status == StatusError {
		return NewTransferError(response)
	}
	if response.Status == StatusDone {
		if response != nil && response.WriteCount > 0 {
			transferJob.SetDestCount(response.WriteCount)
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

	transferJob := session.buildTransferJob(partition, chunk.CriteriaValues, chunk.Suffix, chunk.Count())
	session.Job.Add(transferJob)
	err := s.transferDataWithRetries(session, transferJob)
	chunk.Transfer = transferJob
	chunk.Status = StatusOk
	if err == nil {
		err = s.appendData(session, chunk.Suffix, partition.Suffix)
	}
	if err != nil {
		chunk.Status = StatusError
		return
	}
}

func (s *service) mergeData(session *Session, suffix string, criteriaValues map[string]interface{}) error {
	DML, err := session.Builder.DML(session.Request.MergeStyle, suffix, criteriaValues)
	if s.Config.Debug {
		log.Printf("DML: %v\n", DML)
	}
	dialect := dsc.GetDatastoreDialect(session.DestDB.Config().DriverName)
	dbName := session.getDbName(session.DestDB)
	if err == nil {
		if _, err = session.DestDB.Execute(DML); err == nil {
			err = dialect.DropTable(session.DestDB, dbName, session.Builder.Table(suffix))
		}
	}
	return err
}

func (s *service) mergePartitionData(session *Session, partition *Partition) error {
	session.Partitions.Lock()
	defer session.Partitions.Unlock()
	return s.mergeData(session, partition.Suffix, partition.criteriaValues)
}

func (s *service) appendData(session *Session, sourceSuffix, destSuffix string) error {
	DML := session.Builder.AppendDML(sourceSuffix, destSuffix)
	if s.Config.Debug {
		log.Printf("DML: %v\n", DML)
	}
	dialect := dsc.GetDatastoreDialect(session.DestDB.Config().DriverName)
	dbName, _ := dialect.GetCurrentDatastore(session.DestDB)
	var err error
	if _, err = session.DestDB.Execute(DML); err == nil {
		err = dialect.DropTable(session.DestDB, dbName, session.Builder.Table(sourceSuffix))
	}
	return err
}

func (s *service) deletePartitionData(session *Session, partition *Partition) error {
	return s.deleteData(session, partition.Suffix, partition.criteriaValues)
}

func (s *service) deleteData(session *Session, suffix string, criteriaValues map[string]interface{}) error {
	DML, err := session.Builder.DML(DMLDelete, suffix, criteriaValues)
	if s.Config.Debug {
		log.Printf("DML: %v\n", DML)
	}
	if err != nil {
		return err
	}
	_, err = session.DestDB.Execute(DML)
	return err
}

func (s *service) syncDataPartitions(session *Session) error {
	if !session.Request.Force {
		session.Job.Stage = "batching partitions sync status"
		if err := session.BatchSyncInfo(); err != nil {
			return err
		}
	}
	session.Job.Stage = "processing partition"
	return session.Partitions.Range(func(partition *Partition) error {
		var err error
		if !session.Request.Force {
			info, err := session.GetSyncInfo(partition.criteriaValues)
			if err != nil {
				return err
			}
			partition.SourceCount = info.SourceCount
			if info.InSync {
				return nil
			}
			session.SetSynMethod(info.Method)
			if info.SyncFromID > 0 {
				partition.criteriaValues[session.Builder.UniqueColumns[0]] = &greaterThan{value: info.SyncFromID}
			}
		}

		if session.isChunkedTransfer {
			err = s.syncDataPartitionWithChunks(session, partition)
		} else {
			err = s.syncDataPartition(session, partition)
		}

		if err == nil {
			switch session.syncMethod {
			case SyncMethodInsert:
				err = s.appendData(session, partition.Suffix, "")
			case SyncMethodMergeDelete:
				if err = s.deletePartitionData(session, partition); err != nil {
					return err
				}
				fallthrough
			default:
				err = s.mergePartitionData(session, partition)
			}
		}
		return err
	})
}

func (s *service) syncDataPartition(session *Session, partition *Partition) error {
	if err := s.createTransientDest(session, partition.Suffix); err != nil {
		return err
	}
	transferJob := session.buildTransferJob(partition, partition.criteriaValues, partition.Suffix, partition.SourceCount)
	session.Job.Add(transferJob)
	return s.transferDataWithRetries(session, transferJob)
}

func (s *service) syncDataPartitionWithChunks(session *Session, partition *Partition) error {
	max := 0

	limit := session.Request.Sync.ChunkSize
	if err := s.createTransientDest(session, partition.Suffix); err != nil {
		return err
	}
	go s.transferDataChunks(session, partition)
	partitionCriteria := partition.CriteriaValues()
	for {
		criteriaValues := partition.CriteriaValues()
		partitionCriteria[session.Builder.UniqueColumns[0]] = &greaterOrEqual{max}
		checkDQL, err := session.Builder.ChunkDQL(session.Source, max, limit, partitionCriteria)
		if err != nil {
			return err
		}
		if session.IsDebug() {
			log.Printf("chunk source SQL: %v\n", checkDQL)
		}
		sourceCount := &ChunkInfo{}
		if _, err = session.SourceDB.ReadSingle(sourceCount, checkDQL, nil, nil); err != nil {
			return err
		}
		if session.IsDebug() {
			log.Printf("chunk source data: %v\n", sourceCount)
		}

		if sourceCount.Count() > limit {
			return fmt.Errorf("invalid chunk SQL: %v, count: %v is greater than chunk limit: %v", checkDQL, sourceCount.Count(), limit)
		}

		criteriaValues[session.Builder.UniqueColumns[0]] = &between{from: sourceCount.Min(), to: sourceCount.Max()}

		destDQL := session.Builder.CountDQL("", session.Dest, criteriaValues)
		if session.IsDebug() {
			log.Printf("chunk dest SQL: %v\n", destDQL)
		}
		destCount := &ChunkInfo{}
		if _, err = session.DestDB.ReadSingle(destCount, destDQL, nil, nil); err != nil {
			return err
		}

		if session.IsDebug() {
			log.Printf("chunk dest data: %v\n", destCount)
		}
		if sourceCount.Count() == 0 {
			break
		}
		max = sourceCount.Max() + 1
		inSync := destCount.Count() == sourceCount.Count()
		if session.IsDebug() {
			log.Printf("count sync: [%v .. %v]: %v (%v, %v) \n", sourceCount.Min(), sourceCount.Max(), inSync, destCount.Count(), sourceCount.Count())
		}
		if session.syncMethod != SyncMethodMergeDelete && ! session.Request.Force {

			if inSync {
				if session.Request.CountOnly {
					log.Printf("[%v .. %v] in sync skipping",  sourceCount.Min(), sourceCount.Max())
					continue
				}
				info, _ := session.GetSyncInfo(criteriaValues)
				if info.InSync {
					session.SetSynMethod(SyncMethodMerge)
					continue
				}
				if info.SyncFromID > 0 {
					criteriaValues[session.Builder.UniqueColumns[0]] = &between{from: info.SyncFromID , to: sourceCount.Max()}
				}

			}
		}

		partition.WaitGroup.Add(1)
		chunk := &Chunk{ChunkInfo: *sourceCount}
		if destCount.Count() == 0 {
			chunk.Method = SyncMethodInsert
		} else {
			session.syncMethod = SyncMethodMerge
			session.SetSynMethod(SyncMethodMerge)
		}
		chunk.CriteriaValues = criteriaValues
		partition.AddChunk(chunk)
	}
	partition.SetDone(1)
	if partition.ChunkSize() > 0 {
		partition.WaitGroup.Wait()
	}
	return nil
}

//New creates a new service or error
func New(config *Config) (Service, error) {
	service := &service{
		Config:       config,
		Jobs:         NewJobs(),
		StatRegistry: NewStatRegistry(config.MaxHistory),
	}
	var err error
	service.scheduler, err = NewScheduler(service, config)
	return service, err
}
