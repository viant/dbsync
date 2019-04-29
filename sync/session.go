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
	SyncMethodMerge = "merge"

	//SyncMethodInsertDelete merge delete sync method
	SyncMethodDeleteInsert = "deleteInsert"

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
	hasID             bool
	hasIDs            bool
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
	if s.syncMethod == SyncMethodMerge {
		return
	}
	s.Job.Method = method
	s.syncMethod = method
}

func (s *Session) hasOnlyDataAppend(sourceData, destData Record, criteria map[string]interface{}, status *Info) bool {
	sourceMaxID := toolbox.AsInt(sourceData[s.Builder.maxIDColumnAlias])
	destMaxID := toolbox.AsInt(destData[s.Builder.maxIDColumnAlias])
	if destMaxID == 0 {
		return false
	}

	narrowedCriteria := cloneMap(criteria)
	narrowedStatus := &Info{}

	if sourceMaxID > destMaxID { //Check if upto the same id data is the same
		narrowedCriteria := cloneMap(criteria)
		narrowedCriteria[s.Builder.IDColumns[0]] = &lessOrEqual{destMaxID}
		narrowedStatus, err := s.GetSyncInfo(narrowedCriteria, false)
		if err != nil {
			return false
		}
		if narrowedStatus.InSync {
			status.Method = SyncMethodInsert
			status.SyncFromID = destMaxID
			status.MinValue = destMaxID + 1
			status.SourceCount -= narrowedStatus.SourceCount
			return true
		}
	}

	if destMaxID, err := s.findInSyncMaxID(destMaxID, narrowedCriteria, narrowedStatus); err == nil && destMaxID > 0 {
		status.Method = SyncMethodMerge
		status.SyncFromID = destMaxID
		status.MinValue = destMaxID + 1
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
		narrowedCriteria[s.Builder.IDColumns[0]] = &lessOrEqual{destMaxID}
		info, err := s.GetSyncInfo(narrowedCriteria, false)
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
	MinValue      int
	MaxValue      int
	depth         int
}

func (s *Session) runDiffSQL(criteria map[string]interface{}, source, dest *[]Record) ([]string, error) {
	destDQL, groupColumns := s.Builder.DiffDQL(criteria, s.Dest)
	sourceSQL, _ := s.Builder.DiffDQL(criteria, s.Source)
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
		log.Printf("[%v] diff source data: %v\n", s.Builder.table, source)
		log.Printf("[%v] diff dest   data: %v\n", s.Builder.table, dest)
	}

	if err := s.Partitions.Validate(*source, *dest); err != nil {
		return nil, fmt.Errorf("[%v] %v", s.Builder.table, err)
	}

	return groupColumns, nil
}

func (s *Session) sumRowCount(records Records) int {
	return records.Sum(s.Builder.countColumnAlias)
}

func (s *Session) sumRowDistinctCount(records Records) int {
	return records.Sum(s.Builder.uniqueCountAlias)
}

func (s *Session) sumRowNotNullDistinctSum(records Records) int {
	return records.Sum(s.Builder.uniqueNotNullSumtAlias)
}

func (s *Session) recordsCumsum(data []Record, column string) int {
	result := 0
	for _, sourceRecord := range data {
		countValue, ok := sourceRecord[column]
		if ok {
			result += toolbox.AsInt(countValue)
		}
	}
	return result
}

func (s *Session) setInfoRange(source, dest map[string]interface{}, info *Info) {
	if !s.hasID {
		return
	}
	maxKey := s.Builder.maxIDColumnAlias
	info.MaxValue = toolbox.AsInt(source[maxKey])
	if info.MaxValue < toolbox.AsInt(dest[maxKey]) {
		info.MaxValue = toolbox.AsInt(dest[maxKey])
	}
	minKey := s.Builder.minIDColumnAlias
	info.MinValue = toolbox.AsInt(source[minKey])
	if destMin := toolbox.AsInt(dest[minKey]); destMin != 0 && destMin < info.MinValue {
		info.MinValue = destMin
	}
}

func (s *Session) isMergeDeleteStrategy(source, dest map[string]interface{}) bool {
	countKey := s.Builder.countColumnAlias
	if toolbox.AsInt(source[countKey]) < toolbox.AsInt(dest[countKey]) {
		return true
	}

	if !IsMapItemEqual(source, dest, countKey) {
		return false
	}
	maxKey := s.Builder.maxIDColumnAlias
	if maxKey == "" {
		return false
	}
	if toolbox.AsInt(source[maxKey]) < toolbox.AsInt(dest[maxKey]) {
		return true
	}
	return false
}

func (s *Session) validateSourceData(sourceRecords []Record, criteria map[string]interface{}) error {
	if !s.hasID {
		return nil
	}
	if s.sumRowDistinctCount(sourceRecords) != s.sumRowNotNullDistinctSum(sourceRecords) {
		return fmt.Errorf("[%v](%v) invalid source data: unique column has NULL values, rowCount: %v, distinct ids: %v, not null ids sum: %v\n",
			s.Request.Table,
			criteria,
			s.sumRowCount(sourceRecords),
			s.sumRowDistinctCount(sourceRecords),
			s.sumRowNotNullDistinctSum(sourceRecords))
	}
	return nil
}

func (s *Session) validateDestinationData(destRecords []Record, criteria map[string]interface{}) error {
	if !s.hasID {
		return nil
	}
	destRowCount := s.sumRowCount(destRecords)
	destDistinctRowCount := s.sumRowDistinctCount(destRecords)
	if destRowCount > destDistinctRowCount {
		destDistinctRowSum := s.sumRowNotNullDistinctSum(destRecords)
		if destDistinctRowSum != destDistinctRowCount {
			return fmt.Errorf("[%v](%v) invalid dest, data has unique duplicates; rowCount: %v, distinct ids: %v\n",
				s.Request.Table,
				criteria,
				destRowCount,
				destDistinctRowCount)
		}
		return fmt.Errorf("[%v](%v) invalid dest, data has unique NULL values; rowCount: %v, distinct ids: %v, distinct sum: %v\n",
			s.Request.Table,
			criteria,
			destRowCount,
			destDistinctRowCount,
			destDistinctRowSum)
	}
	return nil
}

func (s *Session) buildSyncInfo(sourceRecords, destRecords []Record, groupColumns []string, criteria map[string]interface{}, optimizeAppend bool) (*Info, error) {
	var err error
	result := &Info{
		depth: 1,
	}
	defer func() {
		if s.IsDebug() {
			log.Printf("[%v] %v method: %v\n", s.Builder.table, criteria, result.Method)
		}
	}()
	result.SourceCount = s.sumRowCount(sourceRecords)
	if len(destRecords) == 0 {
		result.Method = SyncMethodInsert
		return result, nil
	}
	isEqual := s.IsEqual(groupColumns, sourceRecords, destRecords, result)
	if s.IsDebug() {
		log.Printf("[%v] equal: %v,  %v , %v\n", s.Builder.table, isEqual, sourceRecords, destRecords)
	}
	if isEqual {
		result.InSync = true
		return result, nil
	}
	if err := s.Partitions.Validate(sourceRecords, destRecords); err != nil {
		return nil, fmt.Errorf("[%v] %v", s.Builder.table, err)
	}
	if result.Method != "" {
		return result, nil
	}

	if !s.hasIDs {
		result.Method = SyncMethodDeleteInsert
		return result, nil
	}

	result.Method = SyncMethodMerge
	if len(sourceRecords) == 1 && len(destRecords) == 1 {

		s.setInfoRange(sourceRecords[0], destRecords[0], result)
		if err = s.validateSourceData(sourceRecords, criteria); err == nil {
			err = s.validateDestinationData(destRecords, criteria)
		}
		if err != nil {
			return nil, err
		}

		if s.isMergeDeleteStrategy(sourceRecords[0], destRecords[0]) {
			result.Method = SyncMethodMergeDelete
		} else if optimizeAppend {
			if s.hasOnlyDataAppend(sourceRecords[0], destRecords[0], criteria, result) {
				return result, nil
			}
			result.Method = SyncMethodMerge
		}
	}
	return result, nil
}

//GetSyncInfo returns a sync info
func (s *Session) GetSyncInfo(criteria map[string]interface{}, optimizeAppend bool) (*Info, error) {
	if s.Partitions.hasKey {
		keyValue := keyValue(s.Partitions.key, criteria)
		partition, ok := s.Partitions.index[keyValue]
		if ok && partition.Info != nil {
			return partition.Info, nil
		}
	}

	var sourceData = make([]Record, 0)
	var destData = make([]Record, 0)

	groupColumns, err := s.runDiffSQL(criteria, &sourceData, &destData)
	if err != nil {
		return nil, err
	}

	return s.buildSyncInfo(sourceData, destData, groupColumns, criteria, optimizeAppend)
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
	batchedCriteria := batchCriteria(s.Partitions.data, s.Request.DiffBatchSize)
	if len(batchedCriteria) == 0 {
		return nil
	}

	batchSize := s.Request.Partition.BatchSize(len(batchedCriteria))
	limiter := toolbox.NewBatchLimiter(batchSize, len(batchedCriteria))
	index := newIndexedRecords(s.Partitions.key)
	for _, batchCriteria := range batchedCriteria {
		go func() {
			limiter.Acquire()
			defer limiter.Done()
			if e := s.readSyncInfoBatch(batchCriteria, index); e != nil {
				err = e
			}

		}()
	}
	limiter.Wait()

	for key, partition := range s.Partitions.index {
		sourceRecords, has := index.source[key]
		if !has {
			continue
		}
		destRecords := index.dest[key]
		partition.Info, err = s.buildSyncInfo(sourceRecords, destRecords, s.Partitions.key, partition.criteria, true)
		if err != nil {
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
			for _, column := range s.Builder.Columns {
				key := column.Alias

				if !IsMapItemEqual(destRecord, sourceRecord, key) {
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

func (s *Session) buildTransferJob(partition *Partition, criteria map[string]interface{}, suffix string, sourceCount int) *TransferJob {
	DQL := s.Builder.DQL("", s.Source, criteria, false)
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
		hasID:    len(request.IDColumns) == 1,
		hasIDs:   len(request.IDColumns) > 0,
		Response: response,
		Source:   request.Source,
		SourceDB: sourceDB,
		Dest:     request.Dest,
		DestDB:   destDB,
		Builder:  builder,
	}
	multiChunk := request.Chunk.Size
	if multiChunk == 0 {
		multiChunk = 1
	}
	session.isChunkedTransfer = request.Chunk.Size > 0
	return session, nil
}
