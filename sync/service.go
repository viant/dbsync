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
		for _, partition := range session.Partitions.index {
			syncStats.Methods[partition.Method]++
			syncStats.PartitionTransferred++
		}
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
		partitions = append(partitions, NewPartition(session.Request.Partition, values, session.Request.Chunk.QueueSize, uniqueColumn))
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
	if session.Request.Transfer.TempDatabase != "" {
		_ = dialect.DropTable(session.DestDB, session.Request.Transfer.TempDatabase, table)
	} else {
		_ = dialect.DropTable(session.DestDB, dbName, table)
	}
	DDL, err := session.Builder.DDL(suffix)
	if session.Request.Transfer.TempDatabase != "" {
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

	transferJob := session.buildTransferJob(partition, chunk.Criteria, chunk.Suffix, chunk.Count())
	session.Job.Add(transferJob)
	err := s.transferDataWithRetries(session, transferJob)
	chunk.Transfer = transferJob
	chunk.Status = StatusOk

	if err == nil {
		if chunk.Method == SyncMethodMergeDelete {
			err = s.removeInconsistency(session, chunk, partition)
		}
		if err == nil {
			err = s.appendData(session, chunk.Suffix, partition.Suffix)
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

func (s *service) mergeData(session *Session, suffix string, criteria map[string]interface{}) error {
	DML, err := session.Builder.DML(session.Request.MergeStyle, suffix, criteria)
	session.Log(nil, fmt.Sprintf("DML:\n\t%v", DML))
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
	return s.mergeData(session, partition.Suffix, partition.criteria)
}


func (s *service) appendData(session *Session, sourceSuffix, destSuffix string) error {
	DML := session.Builder.AppendDML(sourceSuffix, destSuffix)
	session.Log(nil, fmt.Sprintf("DML:\n\t%v", DML))
	dialect := dsc.GetDatastoreDialect(session.DestDB.Config().DriverName)
	dbName, _ := dialect.GetCurrentDatastore(session.DestDB)
	var err error
	if _, err = session.DestDB.Execute(DML); err == nil {
		err = dialect.DropTable(session.DestDB, dbName, session.Builder.Table(sourceSuffix))
	}
	return err
}

func (s *service) deletePartitionData(session *Session, partition *Partition) error {
	return s.deleteData(session, partition.Suffix, partition.criteria)
}

func (s *service) deleteData(session *Session, suffix string, criteria map[string]interface{}) error {
	DML, err := session.Builder.DML(DMLDelete, suffix, criteria)
	session.Log(nil, fmt.Sprintf("DML:\n\t%v", DML))
	if err != nil {
		return err
	}
	_, err = session.DestDB.Execute(DML)
	return err
}

func (s *service) syncDataPartitions(session *Session) error {
	optimizeSync := !session.Request.Force

	if optimizeSync  {
		session.Job.Stage = "batching partitions sync status"
		if err := session.BatchSyncInfo(); err != nil {
			return err
		}
	}
	session.Job.Stage = "processing partition"
	return session.Partitions.Range(func(partition *Partition) error {
		var err error

		if !session.Request.Force {
			info, err := session.GetSyncInfo(partition.criteria, true)
			if err != nil {
				return err
			}
			partition.Info = info
			partition.SourceCount = info.SourceCount
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

		session.Log(partition, fmt.Sprintf("sync method: %v", partition.Method))
		if err == nil {
			switch partition.Method {
			case SyncMethodDeleteInsert:
				if err = s.deletePartitionData(session, partition); err != nil {
					return err
				}
				fallthrough
			case SyncMethodInsert:
				err = s.appendData(session, partition.Suffix, "")
				break
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

	transferJob := session.buildTransferJob(partition, partition.criteria, partition.Suffix, partition.SourceCount)
	session.Job.Add(transferJob)
	return s.transferDataWithRetries(session, transferJob)
}





func (s *service) buildChunks(session *Session, partition *Partition) error {
	max := 0
	limit := session.Request.Chunk.Size
	partitionCriteria := partition.CloneCriteria()

	if partition.SyncFromID > 0 { //data is sync upto SyncFromID
		max = partition.SyncFromID + 1
	}

	optimizeSync := ! session.Request.Force
	hasOverflowData := partition.Method == SyncMethodMergeDelete

	for ; ; {


		i := len(partition.chunks)
		criteria := partition.CloneCriteria()
		partitionCriteria[session.Builder.IDColumns[0]] = &greaterOrEqual{max}
		sourceDQL, err := session.Builder.ChunkDQL(session.Source, max, limit, partitionCriteria)
		if err != nil {
			return err
		}
		destDQL, _ := session.Builder.ChunkDQL(session.Dest, max, limit, partitionCriteria)
		sourceInfo := &ChunkInfo{}
		if _, err = session.SourceDB.ReadSingle(sourceInfo, sourceDQL, nil, nil); err != nil {
			session.Error(partition, fmt.Sprintf("failed run source chunk SQL: %v, %v\n", sourceDQL, err))
			return err
		}

		if isLimitSQLBroken := sourceInfo.Count() > limit; isLimitSQLBroken  {
			message := fmt.Sprintf("invalid chunk SQL: %v, count: %v is greater than chunk limit: %v", sourceDQL, sourceInfo.Count(), limit)
			session.Log(partition, message)
			return fmt.Errorf("%s", message)
		}

		minValue := sourceInfo.Min()
		if i == 0 && partition.Info != nil && partition.MinValue < minValue {
			session.Log(partition, fmt.Sprintf("updating chunk min with partition narrowed min: %v\n", partition.MinValue))
			minValue = partition.MinValue
		}



		chunkMax := sourceInfo.Max()
		if ! hasOverflowData || (sourceInfo.Count() == limit) {
			criteria[session.Builder.IDColumns[0]] = &between{from: minValue, to: chunkMax}
			destDQL = session.Builder.CountDQL("", session.Dest, criteria)
		}
		destInfo := &ChunkInfo{}
		if _, err = session.DestDB.ReadSingle(destInfo, destDQL, nil, nil); err != nil {
			session.Error(partition, fmt.Sprintf("failed run dest chunk SQL: %v, %v\n", destDQL, err))
			return err
		}

		noMoreSourceData := sourceInfo.Count() == 0 || sourceInfo.Max() == 0 && destInfo.Count() == 0
		if noMoreSourceData {
			session.Log(partition, fmt.Sprintf("built %d chunks", len(partition.chunks)))
			break
		}
		if destInfo.Max() > chunkMax {
			chunkMax = destInfo.Max()
			criteria[session.Builder.IDColumns[0]] = &between{from: minValue, to: chunkMax}
			session.Log(partition, fmt.Sprintf("updating chunk max: %v\n", chunkMax))
		}

		max = chunkMax + 1
		inSync := destInfo.Count() == sourceInfo.Count() &&
			destInfo.Max() == sourceInfo.Max() &&
			destInfo.Min() == sourceInfo.Min()

		if optimizeSync && inSync {
			if session.Request.Diff.CountOnly {
				session.Log(partition, fmt.Sprintf("chunk[%d]: [%v..%v] countOnly inSync: true", i, sourceInfo.Min(), sourceInfo.Max()))
				continue
			}
			info, _ := session.GetSyncInfo(criteria, true)
			if info.InSync {
				session.Log(partition, fmt.Sprintf("chunk[%d]: [%v..%v] is inSync: true", i, sourceInfo.Min(), sourceInfo.Max()))
				continue
			}
			if info.SyncFromID > 0 {
				criteria[session.Builder.IDColumns[0]] = &between{from: info.SyncFromID, to: chunkMax}
				sourceInfo.MinValue = info.SyncFromID
				destInfo.MinValue = info.SyncFromID
				session.Log(partition, fmt.Sprintf("updating chunk narrowed min: %v -> %v\n", info.SyncFromID, info.Method))
			}
		}

		session.Log(partition, fmt.Sprintf("sourceDQL: %v", sourceDQL))
		session.Log(partition, fmt.Sprintf("destSQL: %v", destDQL))

		partition.WaitGroup.Add(1)
		sourceInfo.MinValue = minValue
		chunk := newChunk(sourceInfo, destInfo, session, partition, criteria)
		session.Log(partition, fmt.Sprintf("chunk[%d]: sync method: %v [%v..%v] is syncing ...\n\tcount:(%d:%d), max:(%d:%d)", i, chunk.Method,
			sourceInfo.Min(), sourceInfo.Max(),
			sourceInfo.Count(), destInfo.Count(),
			sourceInfo.Max(), destInfo.Max()))

		partition.AddChunk(chunk)
	}
	return nil
}

func (s *service) syncDataPartitionWithChunks(session *Session, partition *Partition) error {
	err := s.createTransientDest(session, partition.Suffix)
	if err != nil {
		return err
	}
	go s.transferDataChunks(session, partition)
	if err = s.buildChunks(session, partition); err != nil {
		return err
	}
	partition.SetDone(1)
	if partition.ChunkSize() > 0 {
		partition.WaitGroup.Wait()
	}
	return nil
}

func newChunk(sourceInfo *ChunkInfo, destInfo *ChunkInfo, session *Session, partition *Partition, criteria map[string]interface{}) *Chunk {
	chunk := &Chunk{ChunkInfo: *sourceInfo}
	if destInfo.Count() == 0 {
		chunk.Method = SyncMethodInsert
		partition.SetSynMethod(SyncMethodInsert)
	} else if destInfo.Count() >= sourceInfo.Count() {
		chunk.Method = SyncMethodMergeDelete
		//Deleting record takes place only on the chunked where clause, thus session sync method is merge
		partition.SetSynMethod(SyncMethodMerge)
	} else {
		chunk.Method = SyncMethodMerge
		partition.SetSynMethod(SyncMethodMerge)
	}
	chunk.Criteria = criteria
	return chunk
}

//TODO MERGE_DELETE ONLY ON THE CHUNK LEVEL use case optimization

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
