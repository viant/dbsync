package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

const (
	//SyncMethodInsert insert sync method
	SyncMethodInsert = "insert"
	//SyncMethodMergeDelete merge delete sync method
	SyncMethodMergeDelete = "mergeDelete"
	//SyncMethodMerge merge sync method
	SyncMethodMerge      = "merge"
	defaultDiffBatchSize = 512
)

//Session represents a ssession
type Session struct {
	Job        *Job
	Response   *Response
	Request    *Request
	Builder    *Builder
	Source     *Resource
	Dest       *Resource
	SourceDB   dsc.Manager
	DestDB     dsc.Manager
	Partitions *Partitions
	*Config
	isChunkedTransfer bool
	syncMethod        string
	err               error
	closed            uint32
}

//IsDebug returns true if debug is on
func (s *Session) IsDebug() bool {
	return s.Config.Debug || s.Request.Debug
}

//SetError sets an error
func (s *Session) SetError(err error) bool {
	if err == nil || s.err != nil {
		return err != nil
	}
	s.err = err
	atomic.StoreUint32(&s.closed, 1)
	s.Response.SetError(err)
	s.Job.Error = err.Error()
	s.SetStatus(StatusError)
	return true
}

//SetStatus sets a status
func (s *Session) SetStatus(status string) {
	s.Response.Status = status
	s.Job.Status = status
	if status == StatusError || status == StatusDone {
		now := time.Now()
		s.Job.EndTime = &now
	}
}

//IsClosed returns true if closed
func (s *Session) IsClosed() bool {
	return atomic.LoadUint32(&s.closed) == 1
}

//SetSynMethod sets sync method
func (s *Session) SetSynMethod(method string) {
	if s.syncMethod == SyncMethodMergeDelete {
		return
	}
	s.Job.Method = method
	s.syncMethod = method
}

func (s *Session) hasOnlyAddition(sourceData, destData Record, criteriaValue map[string]interface{}, status *Info) bool {
	if s.Builder.maxIDColumnAlias == "" ||
		(s.Partitions.keyColumn != "" && len(criteriaValue) > 1) ||
		(s.Partitions.keyColumn == "" && len(criteriaValue) > 0) {
		return false
	}

	sourceMaxID := toolbox.AsInt(sourceData[s.Builder.maxIDColumnAlias])
	destMaxID := toolbox.AsInt(destData[s.Builder.maxIDColumnAlias])
	if destMaxID == 0 {
		return false
	}

	narrowedCriteria := cloneMap(criteriaValue)
	narrowedStatus := &Info{}
	if sourceMaxID > destMaxID { //Check if upto the same id data is the same
		narrowedCriteria := cloneMap(criteriaValue)
		narrowedCriteria[s.Builder.UniqueColumns[0]] = &lessOrEqual{destMaxID}
		narrowedStatus, err := s.GetSyncInfo(narrowedCriteria)
		if err != nil {
			return false
		}
		if narrowedStatus.InSync {
			status.Method = SyncMethodInsert
			status.SyncFromID = destMaxID
			status.SourceCount -= narrowedStatus.SourceCount
			return true
		}
	}

	if destMaxID, err := s.findInSyncMaxID(destMaxID, narrowedCriteria, narrowedStatus); err == nil && destMaxID > 0 {
		status.Method = SyncMethodMerge
		status.SyncFromID = destMaxID
		status.SourceCount -= narrowedStatus.SourceCount
		return true
	}
	return false
}

func (s *Session) findInSyncMaxID(destMaxID int, narrowedCriteria map[string]interface{}, status *Info) (int, error) {
	if s.Request.DiffDepth == 0 {
		return 0, nil
	}
	inSyncDestMaxID := 0
	destMaxID = int(float64(destMaxID) * 0.5)
	delta := int(float64(destMaxID) * 0.5)
	for i := 0; i < s.Request.DiffDepth; i++ {
		if destMaxID <= 0 {
			break
		}
		narrowedCriteria[s.Builder.UniqueColumns[0]] = &lessOrEqual{destMaxID}
		info, err := s.GetSyncInfo(narrowedCriteria)
		if err != nil {
			return 0, nil
		}
		if info.InSync {
			if destMaxID > inSyncDestMaxID {
				inSyncDestMaxID = destMaxID
				status.SourceCount = info.SourceCount
			}
			destMaxID += delta
		} else {
			destMaxID -= delta
		}
		delta = int(float64(delta) * 0.5)
	}
	return inSyncDestMaxID, nil
}

//Info represents a sync info
type Info struct {
	InSync        bool
	Method        string
	Inconsistency string
	SourceCount   int
	SyncFromID    int
	depth         int
}

func (s *Session) runDiffSQL(criteriaValue map[string]interface{}, source, dest *[]Record) ([]string, error) {
	destDQL, groupColumns := s.Builder.DiffDQL(criteriaValue, s.Dest)

	sourceSQL, _ := s.Builder.DiffDQL(criteriaValue, s.Source)
	if s.IsDebug() {
		log.Printf("diff SQL: %v\n", sourceSQL)
	}

	err := s.DestDB.ReadAll(dest, destDQL, nil, nil)
	if err != nil {
		return nil, err
	}
	if err = s.SourceDB.ReadAll(source, sourceSQL, nil, nil); err != nil {
		return nil, err
	}
	if s.IsDebug() {
		log.Printf("diff source data: %v\n", source)
		log.Printf("diff dest   data: %v\n", dest)
	}
	return groupColumns, nil
}

func (s *Session) sumRowCount(data []Record) int {
	result := 0
	for _, sourceRecord := range data {
		countValue, ok := sourceRecord[s.Builder.countColumnAlias]
		if ok {
			result += toolbox.AsInt(countValue)
		}
	}
	return result
}
func (s *Session) sumRowDistinctCount(data []Record) int {
	result := 0
	if s.Builder.uniqueCountAlias == "" {
		return -1
	}
	for _, sourceRecord := range data {
		countValue, ok := sourceRecord[s.Builder.uniqueCountAlias]
		if ok {
			result += toolbox.AsInt(countValue)
		}
	}
	return result
}

func (s *Session) buildSyncInfo(sourceData, destData []Record, groupColumns []string, criteriaValue map[string]interface{}) (*Info, error) {
	result := &Info{
		depth: 1,
	}
	defer func() {
		if s.IsDebug() {
			log.Printf("%v method: %v\n", criteriaValue, result.Method)
		}
	}()
	result.SourceCount = s.sumRowCount(sourceData)
	if len(destData) == 0 {
		result.Method = SyncMethodInsert
		return result, nil
	}

	isEqual := s.IsEqual(groupColumns, sourceData, destData, result)
	if s.IsDebug() {
		log.Printf("equal: %v,  %v , %v\n", isEqual, sourceData, destData)
	}
	if isEqual {
		result.InSync = true
		return result, nil
	}

	if result.Method != "" {
		return result, nil
	}
	result.Method = SyncMethodMerge
	if len(sourceData) == 1 && len(destData) == 1 {

		if toolbox.AsInt(sourceData[0][s.Builder.countColumnAlias]) < toolbox.AsInt(destData[0][s.Builder.countColumnAlias]) {
			destRowCount := s.sumRowCount(destData)
			destDistinctRowCount := s.sumRowDistinctCount(destData)
			if destRowCount > destDistinctRowCount {
				return nil, fmt.Errorf("destination %v[%v], has duplicates rowCount: %v, distinct ids: %v\n", s.Request.Table, criteriaValue, destRowCount, destDistinctRowCount)
			}
			result.Method = SyncMethodMergeDelete
		} else {
			if s.hasOnlyAddition(sourceData[0], destData[0], criteriaValue, result) {
				return result, nil
			}
			result.Method = SyncMethodMerge
		}
	}
	return result, nil
}

//GetSyncInfo returns a sync info
func (s *Session) GetSyncInfo(criteriaValue map[string]interface{}) (*Info, error) {
	if len(criteriaValue) == 1 {
		if value, has := criteriaValue[s.Partitions.keyColumn]; has {
			partition, ok := s.Partitions.index[toolbox.AsString(value)]
			if ok && partition.Info != nil {
				return partition.Info, nil
			}
		}
	}

	var sourceData = make([]Record, 0)
	var destData = make([]Record, 0)

	groupColumns, err := s.runDiffSQL(criteriaValue, &sourceData, &destData)
	if err != nil {
		return nil, err
	}

	return s.buildSyncInfo(sourceData, destData, groupColumns, criteriaValue)
}

func (s *Session) readSyncInfoBatch(batchCriteria map[string]interface{}, index *indexedRecords) error {
	var sourceData = make([]Record, 0)
	var destData = make([]Record, 0)
	_, err := s.runDiffSQL(batchCriteria, &sourceData, &destData)
	if err != nil {
		return err
	}
	index.build(sourceData, index.source)
	index.build(destData, index.dest)
	return nil
}

//BatchSyncInfo returns batch sync info
func (s *Session) BatchSyncInfo() error {
	var err error
	batchedCriteria := batchCriteria(s.Partitions.data, s.Request.Sync.DiffBatchSize)
	if len(batchedCriteria) == 0 {
		return nil
	}
	batchSize := s.Request.Partition.BatchSize(len(batchedCriteria))
	limiter := toolbox.NewBatchLimiter(batchSize, len(batchedCriteria))
	index := newIndexedRecords(s.Partitions.keyColumn)
	groupColumns := []string{s.Partitions.keyColumn}
	for _, batchCriteria := range batchedCriteria {
		go func() {
			limiter.Acquire()
			defer limiter.Done()
			if e := s.readSyncInfoBatch(batchCriteria, index); e != nil {
				err = e
			}

		}()
	}

	for key, partition := range s.Partitions.index {
		sourceRecords, has := index.source[key]
		if !has {
			continue
		}
		destRecords := index.dest[key]
		if partition.Info, err = s.buildSyncInfo(sourceRecords, destRecords, groupColumns, partition.criteriaValues); err != nil {
			return err
		}
	}
	return nil
}

//IsEqual checks if source and dest dataset is equal
func (s *Session) IsEqual(index []string, source, dest []Record, status *Info) bool {
	indexedSource := indexBy(source, index)
	indexedDest := indexBy(dest, index)
	for key := range indexedSource {
		sourceRecord := indexedSource[key]
		destRecord, ok := indexedDest[key]
		if !ok {
			return false
		}
		discrepant := false
		for k, v := range sourceRecord {

			if destRecord[k] != v {
				discrepant = true
				break
			}
		}
		if discrepant { //Try apply date format or numeric rounding to compare again
			for _, column := range s.Builder.Sync.Columns {
				key := column.Alias

				if destRecord[key] != nil && sourceRecord[key] != nil {
					if toolbox.IsInt(destRecord[key]) || toolbox.IsInt(sourceRecord[key]) {
						destRecord[key] = toolbox.AsInt(destRecord[key])
						sourceRecord[key] = toolbox.AsInt(sourceRecord[key])
					} else if toolbox.IsFloat(destRecord[key]) || toolbox.IsFloat(sourceRecord[key]) {
						destRecord[key] = toolbox.AsFloat(destRecord[key])
						sourceRecord[key] = toolbox.AsFloat(sourceRecord[key])
					} else if toolbox.IsBool(destRecord[key]) || toolbox.IsBool(sourceRecord[key]) {
						destRecord[key] = toolbox.AsBoolean(destRecord[key])
						sourceRecord[key] = toolbox.AsBoolean(sourceRecord[key])
					}
				}
				if destRecord[key] != sourceRecord[key] {
					if column.DateLayout != "" {
						destTime, err := toolbox.ToTime(destRecord[key], column.DateLayout)
						if err != nil {
							return false
						}
						sourceTime, err := toolbox.ToTime(sourceRecord[key], column.DateLayout)
						if err != nil {
							return false
						}
						if destTime.Format(column.DateLayout) != sourceTime.Format(column.DateLayout) {
							return false
						}
					} else if column.NumericPrecision > 0 {
						if round(destRecord[key], column.NumericPrecision) != round(sourceRecord[key], column.NumericPrecision) {
							return false
						}
					} else {
						return false
					}
				}
			}
		}
	}
	return true
}

func (s *Session) getDbName(manager dsc.Manager) string {
	dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
	dbName, _ := dialect.GetCurrentDatastore(manager)
	return dbName
}

func (s *Session) destConfig() *dsc.Config {
	if s.Request.TempDatabase == "" {
		return s.Dest.Config
	}
	result := *s.Dest.Config
	result.Parameters = make(map[string]interface{})
	dbName := s.getDbName(s.DestDB)
	for k, v := range s.Dest.Config.Parameters {
		result.Parameters[k] = v
		if textValue, ok := v.(string); ok {
			result.Parameters[k] = strings.Replace(textValue, dbName, s.Request.TempDatabase, 1)
		}
	}
	result.Descriptor = strings.Replace(result.Descriptor, dbName, s.Request.TempDatabase, 1)
	return &result
}

func (s *Session) buildTransferJob(partition *Partition, criteriaValue map[string]interface{}, suffix string, sourceCount int) *TransferJob {
	DQL := s.Builder.DQL("", s.Source, criteriaValue, false)
	if s.IsDebug() {
		log.Printf("DQL:%v\n", DQL)
	}

	destTable := s.Builder.Table(suffix)
	if s.Request.TempDatabase != "" {
		destTable = strings.Replace(destTable, s.Request.TempDatabase+".", "", 1)
	}
	transferRequest := &TransferRequest{
		Source: &Source{
			Config: s.Source.Config,
			Query:  DQL,
		},
		Dest: &Dest{
			Table:  destTable,
			Config: s.destConfig(),
		},
		Async:       s.Request.Async,
		WriterCount: s.Request.WriterThreads,
		BatchSize:   s.Request.BatchSize,
		Mode:        "insert",
	}

	return &TransferJob{
		StartTime: time.Now(),
		Progress: Progress{
			SourceCount: sourceCount,
		},
		MaxRetries:      s.Request.MaxRetries,
		TransferRequest: transferRequest,
		Suffix:          suffix,
		StatusURL:       fmt.Sprintf(transferStatusURL, s.Request.EndpointIP),
		TargetURL:       fmt.Sprintf(transferURL, s.Request.Transfer.EndpointIP),
	}

}

//Close closes session
func (s *Session) Close() {
	_ = s.SourceDB.ConnectionProvider().Close()
	_ = s.DestDB.ConnectionProvider().Close()
}

//NewSession creates a new session
func NewSession(request *Request, response *Response, config *Config) (*Session, error) {
	destDB, err := dsc.NewManagerFactory().Create(request.Dest.Config)
	if err != nil {
		return nil, err
	}
	sourceDB, err := dsc.NewManagerFactory().Create(request.Source.Config)
	if err != nil {
		return nil, err
	}
	builder, err := NewBuilder(request, destDB)
	if err != nil {
		return nil, err
	}
	job := NewJob(request.ID())
	var session = &Session{
		Job:      job,
		Config:   config,
		Request:  request,
		Response: response,
		Source:   request.Source,
		SourceDB: sourceDB,
		Dest:     request.Dest,
		DestDB:   destDB,
		Builder:  builder,
	}
	multiChunk := request.Sync.ChunkSize
	if multiChunk == 0 {
		multiChunk = 1
	}
	session.isChunkedTransfer = request.Sync.ChunkSize > 0
	return session, nil
}
