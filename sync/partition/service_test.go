package partition

import (
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/history"
	"dbsync/sync/jobs"
	"dbsync/sync/model"
	"dbsync/sync/model/strategy/pseudo"
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
		caseDataURI   string
		iDColumns     []string
		partitions    []string
		syncMode      string
		pseudoColumns []*pseudo.Column
		transferError error
		transferred   int
		partitionSQL  string
		expect        interface{}
	}{
		{
			description:  "individual sync mode, with ID, where partitions: 3: merge, 4: insert, 5:delete",
			caseDataURI:  "single_with_id_individual",
			partitions:   []string{"event_type"},
			iDColumns:    []string{"id"},
			partitionSQL: "SELECT event_type FROM events1 GROUP BY 1",
		},
		{
			description:  "batch with ID, where partitions: 3: merge, 4: insert, 5:delete",
			caseDataURI:  "single_with_id_batch",
			partitions:   []string{"event_type"},
			syncMode:     shared.SyncModeBatch,
			iDColumns:    []string{"id"},
			partitionSQL: "SELECT event_type FROM events1 GROUP BY 1",
		},

		{
			/*
				The following partitions needs to be sync
				"2019-04-10_3": "merge",
				"2019-03-28_4": "insert",
				"2019-04-23_5": "deleteMerge"
			*/
			description: "multi key with pseudo column (individual)",
			caseDataURI: "multikey_individual",
			partitions:  []string{"date", "event_type"},
			iDColumns:   []string{"id"},
			pseudoColumns: []*pseudo.Column{
				{
					Name:       "date",
					Expression: "DATE(t.timestamp)",
				},
			},
			partitionSQL: "SELECT event_type, DATE(timestamp) AS date FROM events1 GROUP BY 1, 2",
		},
	}

	ctx := &shared.Context{Debug: false}
	for _, useCase := range useCases {

		if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/sync/%v/prepare", useCase.caseDataURI)), "", "")
		if ! dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}

		dbSync := &model.Sync{
			Source: &model.Resource{Table: "events1", Config: partitionerTestConfig},
			Dest:   &model.Resource{Table: "events2", Config: partitionerTestConfig},
			Table:  "events2",
		}

		dbSync.Source.PseudoColumns = useCase.pseudoColumns
		dbSync.Dest.PseudoColumns = useCase.pseudoColumns
		dbSync.IDColumns = useCase.iDColumns
		dbSync.Partition.Columns = useCase.partitions
		dbSync.Partition.ProviderSQL = useCase.partitionSQL
		dbSync.Partition.SyncMode = useCase.syncMode

		err := dbSync.Init()
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		service := dao.New(dbSync)
		err = service.Init(ctx)
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}

		partitioner := New(dbSync, service, shared.NewMutex(), jobService, historyService)
		//Mock transfer service
		partitioner.Transfer = transfer.NewFaker(partitioner.Transfer, useCase.transferred, useCase.transferError)
		err = partitioner.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		err = partitioner.Build(ctx)
		if ! assert.Nil(t, err, useCase) {
			continue
		}
		err = partitioner.Sync(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		expectDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/sync/%v/expect", useCase.caseDataURI)), "", "")
		dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.FullTableDatasetCheckPolicy, expectDataset))

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
		partitionSQL  string
		expectCount   int
		inSyncCount   int
		expect        interface{}
	}{

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
	}

	ctx := &shared.Context{}
	for _, useCase := range useCases {
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/build/%v", useCase.caseDataURI)), "", "")
		dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

		dbSync := &model.Sync{
			Source: &model.Resource{Table: "events1", Config: partitionerTestConfig},
			Dest:   &model.Resource{Table: "events2", Config: partitionerTestConfig},
			Table:  "events2",
		}
		dbSync.Source.PseudoColumns = useCase.pseudoColumns
		dbSync.Dest.PseudoColumns = useCase.pseudoColumns
		dbSync.IDColumns = useCase.iDColumns
		dbSync.Partition.Columns = useCase.partitions
		dbSync.Partition.ProviderSQL = useCase.partitionSQL

		err := dbSync.Init()
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		service := dao.New(dbSync)
		err = service.Init(ctx)
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}

		partitioner := New(dbSync, service, shared.NewMutex(), jobService, historyService)
		err = partitioner.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		err = partitioner.Build(ctx)
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

		if !assertly.AssertValues(t, useCase.expect, actual) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}
