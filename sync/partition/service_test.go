package partition

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/history"
	"dbsync/sync/jobs"

	"dbsync/sync/contract/strategy/pseudo"
	"dbsync/sync/shared"
	"dbsync/sync/transfer"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"sync"
	"testing"
)

var partitionerTestConfig *dsc.Config
var jobService jobs.Service
var historyService history.Service

func init() {
	parent := toolbox.CallerDirectory(3)
	partitionerTestConfig = &dsc.Config{
		DriverName: "sqlite3",
		Descriptor: path.Join(parent, "test/db/mydb"),
	}
	_ = partitionerTestConfig.Init()

	jobService = jobs.New()
	historyService = history.New(&shared.Config{})
}

func TestPartitioner_Sync(t *testing.T) {

	parent := toolbox.CallerDirectory(3)

	var useCases = []struct {
		description   string
		caseURI       string
		iDColumns     []string
		partitions    []string
		syncMode      string
		pseudoColumns []*pseudo.Column
		directAppend  bool
		transferError error
		chunkSize     int
		filter        map[string]interface{}
		transferred   int
		sourceSQL     string
		destSQL       string
		expect        interface{}
		hasError      bool
	}{

		{
			/*
				The following partitions needs to be sync


				{
					"2" - partition does not exists in source
					"3": "merge",
					"4": "insert",
					"5": "deleteInsert"
				}
			*/
			description: "partition with id based - partition removal ",
			caseURI:     "single_with_id_removal",
			partitions:  []string{"event_type"},
			iDColumns:   []string{"id"},
			sourceSQL:   "SELECT event_type FROM events1 GROUP BY 1",
			destSQL:     "SELECT event_type FROM events2 GROUP BY 1",
		},

		{

			description: "chunk",
			caseURI:     "chunked",
			iDColumns:   []string{"id"},
			chunkSize:   3,
		},

		{
			description:  "chunk - direct",
			caseURI:      "chunked_direct",
			iDColumns:    []string{"id"},
			directAppend: true,
			chunkSize:    3,
		},

		{
			/*
				The following partitions needs to be sync
				{
					"3": "merge",
					"4": "insert",
					"5": "deleteInsert"
				}
			*/
			description: "partition with id based - individual",
			caseURI:     "single_with_id_individual",
			partitions:  []string{"event_type"},
			iDColumns:   []string{"id"},
			sourceSQL:   "SELECT event_type FROM events1 GROUP BY 1",
		},

		{
			/*
				The following partitions needs to be sync
				{
					"3": "merge",
					"4": "insert",
					"5": "deleteInsert"
				}
			*/

			description: "partition with id based - batched",
			caseURI:     "single_with_id_batch",
			partitions:  []string{"event_type"},
			syncMode:    shared.SyncModeBatch,
			iDColumns:   []string{"id"},
			sourceSQL:   "SELECT event_type FROM events1 GROUP BY 1",
		},

		{
			description:  "partition with id based - batched - direct append",
			caseURI:      "single_with_id_batch_direct",
			directAppend: true,
			partitions:   []string{"event_type"},
			syncMode:     shared.SyncModeBatch,
			iDColumns:    []string{"id"},
			sourceSQL:    "SELECT event_type FROM events1 GROUP BY 1",
		},
		{
			/*
				The following partitions needs to be sync
				"2019-04-10_3": "merge",
				"2019-03-28_4": "insert",
				"2019-04-23_5": "deleteMerge"
			*/
			description: "multi key with pseudo column (individual)",
			caseURI:     "multikey_individual",
			partitions:  []string{"date", "event_type"},
			iDColumns:   []string{"id"},
			pseudoColumns: []*pseudo.Column{
				{
					Name:       "date",
					Expression: "DATE(t.timestamp)",
				},
			},
			sourceSQL: "SELECT event_type, DATE(timestamp) AS date FROM events1 GROUP BY 1, 2",
		},

		{
			description: "chunk error (missing tables)",
			caseURI:     "chunked_err",
			iDColumns:   []string{"id"},
			partitions:  []string{"event_type"},
			sourceSQL:   "SELECT event_type FROM events1 GROUP BY 1",
			chunkSize:   3,
			hasError:    true,
		},

		{
			description:   "chunk error - transfer error",
			caseURI:       "chunked_err",
			iDColumns:     []string{"id"},
			partitions:    []string{"event_type"},
			sourceSQL:     "SELECT event_type FROM events1 GROUP BY 1",
			chunkSize:     3,
			transferError: fmt.Errorf("test error"),
			hasError:      true,
		},
		{

			description:   "partition with id based - error",
			caseURI:       "single_with_id_individual",
			partitions:    []string{"event_type"},
			iDColumns:     []string{"id"},
			sourceSQL:     "SELECT event_type FROM events1 GROUP BY 1",
			transferError: fmt.Errorf("test error"),
			hasError:      true,
		},
	}

	ctx := &shared.Context{Debug: false}
	for _, useCase := range useCases {

		if !dsunit.InitFromURL(t, path.Join(parent, fmt.Sprintf("test/sync/cases/%v/config.yaml", useCase.caseURI))) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/sync/cases/%v/prepare", useCase.caseURI)), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}

		dbSync := &contract.Sync{
			Source: &contract.Resource{Table: "events1", Config: partitionerTestConfig},
			Dest:   &contract.Resource{Table: "events2", Config: partitionerTestConfig},
			Table:  "events2",
		}

		dbSync.Source.PseudoColumns = useCase.pseudoColumns
		dbSync.Dest.PseudoColumns = useCase.pseudoColumns
		dbSync.DirectAppend = useCase.directAppend
		dbSync.IDColumns = useCase.iDColumns
		dbSync.Partition.Columns = useCase.partitions
		dbSync.Source.PartitionSQL = useCase.sourceSQL
		dbSync.Dest.PartitionSQL = useCase.destSQL

		dbSync.Partition.SyncMode = useCase.syncMode
		dbSync.Chunk.Size = useCase.chunkSize
		dbSync.Criteria = useCase.filter

		err := dbSync.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		service := dao.New(dbSync)
		err = service.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		partitioner := newService(dbSync, service, shared.NewMutex(), jobService, historyService)
		defer partitioner.Close()
		//Mock transfer service
		partitioner.Transfer = transfer.NewFaker(partitioner.Transfer, useCase.transferred, useCase.transferError)
		err = partitioner.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		err = partitioner.Build(ctx)
		if !assert.Nil(t, err, useCase) {
			continue
		}
		err = partitioner.Sync(ctx)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		expectDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/sync/cases/%v/expect", useCase.caseURI)), "", "")
		if !dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.FullTableDatasetCheckPolicy, expectDataset)) {
			t.Logf("failed : %v", useCase.description)
		}

	}

}

func TestPartitioner_Build(t *testing.T) {

	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}

	var useCases = []struct {
		description   string
		caseDataURI   string
		iDColumns     []string
		partitions    []string
		pseudoColumns []*pseudo.Column
		sourceFilter  map[string]interface{}
		destFilter    map[string]interface{}
		partitionSQL  string
		expectCount   int
		inSyncCount   int
		force         bool
		hasError      bool
		expect        interface{}
	}{

		{
			description: "no partition - source filter error",
			caseDataURI: "nopartition",
			partitions:  []string{},
			iDColumns:   []string{"id"},
			sourceFilter: map[string]interface{}{
				"t.foo": 1,
			},
			hasError: true,
		},
		{
			description: "no partition - dest filter error",
			caseDataURI: "nopartition",
			partitions:  []string{},
			iDColumns:   []string{"id"},
			destFilter: map[string]interface{}{
				"t.foo": 1,
			},
			hasError: true,
		},

		{
			description:  "single key partition with id",
			caseDataURI:  "single_with_id",
			partitions:   []string{"event_type"},
			iDColumns:    []string{"id"},
			partitionSQL: "SELECT event_type FROM events1 GROUP BY 1",
			inSyncCount:  1,
			expectCount:  3,
			expect: `{
	"3": "merge",
	"4": "insert",
	"5": "deleteMerge"
}`,
		},
		{
			description:  "single key partition with no id",
			caseDataURI:  "single_with_noid",
			partitions:   []string{"event_type"},
			partitionSQL: "SELECT event_type FROM events1 GROUP BY 1",
			inSyncCount:  1,
			expectCount:  3,
			expect: `{
	"3": "deleteInsert",
	"4": "deleteInsert",
	"5": "deleteInsert"}`,
		},

		{
			description: "multi key with pseudo column",
			caseDataURI: "single_with_id",
			partitions:  []string{"date", "event_type"},
			iDColumns:   []string{"id"},
			pseudoColumns: []*pseudo.Column{
				{
					Name:       "date",
					Expression: "DATE(t.timestamp)",
				},
			},
			partitionSQL: "SELECT event_type, DATE(timestamp) AS date FROM events1 GROUP BY 1, 2",
			inSyncCount:  8,
			expectCount:  3,
			expect: `{
		"2019-03-21_3": "insert",
		"2019-03-28_4": "insert",
		"2019-04-23_5": "deleteMerge"}`,
		},
		{
			description: "no partition",
			caseDataURI: "nopartition",
			partitions:  []string{},
			iDColumns:   []string{"id"},
			expectCount: 1,
			expect: `{
				"": "merge"
}`,
		},

		{
			description: "invalid partition SQL",
			caseDataURI: "single_with_id",
			partitions:  []string{"date", "event_type"},
			iDColumns:   []string{"id"},
			pseudoColumns: []*pseudo.Column{
				{
					Name:       "date",
					Expression: "DATE(t.timestamp)",
				},
			},
			partitionSQL: "SELECT abc, DATE(timestamp) AS date FROM events1 GROUP BY 1, 2",
			hasError:     true,
		},
		{
			description: "non optimized",
			caseDataURI: "single_with_id",
			iDColumns:   []string{"id"},
			expectCount: 1,
			force:       true,
			expect: `{
	"": "deleteMerge"
}`,
		},
	}

	ctx := &shared.Context{Debug: false}
	for _, useCase := range useCases {
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/build/cases/%v", useCase.caseDataURI)), "", "")
		dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

		dbSync := &contract.Sync{
			Source: &contract.Resource{Table: "events1", Config: partitionerTestConfig},
			Dest:   &contract.Resource{Table: "events2", Config: partitionerTestConfig},
			Table:  "events2",
		}
		dbSync.Source.PseudoColumns = useCase.pseudoColumns
		dbSync.Dest.PseudoColumns = useCase.pseudoColumns
		dbSync.IDColumns = useCase.iDColumns
		dbSync.Partition.Columns = useCase.partitions
		dbSync.Source.PartitionSQL = useCase.partitionSQL
		dbSync.Force = useCase.force
		err := dbSync.Init()
		dbSync.Source.Criteria = useCase.sourceFilter
		dbSync.Dest.Criteria = useCase.destFilter

		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		service := dao.New(dbSync)
		err = service.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		partitioner := newService(dbSync, service, shared.NewMutex(), jobService, historyService)
		defer partitioner.Close()
		err = partitioner.Init(ctx)
		if err == nil {
			err = partitioner.Build(ctx)
		}

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue

		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		actual := map[string]string{}
		mutex := &sync.Mutex{}
		actualInSyncCount := 0

		_ = partitioner.Partitions.Range(func(partition *core.Partition) error {
			if partition.InSync {
				actualInSyncCount++
				return nil
			}
			mutex.Lock()
			defer mutex.Unlock()
			actual[partition.Filter.Index(useCase.partitions)] = partition.Status.Method
			return nil
		})

		assert.EqualValues(t, useCase.inSyncCount, actualInSyncCount, useCase.description)
		assert.EqualValues(t, useCase.expectCount, len(actual), useCase.description)

		if !assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}
