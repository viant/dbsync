package chunk

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/dao"
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
	"testing"
)

var (
	chunkerTestConfig *dsc.Config
	jobService        jobs.Service
)

func initTest() {
	parent := toolbox.CallerDirectory(3)
	chunkerTestConfig = &dsc.Config{
		DriverName: "sqlite3",
		Descriptor: path.Join(parent, "test/db/mydb"),
	}
	_ = chunkerTestConfig.Init()
	jobService = jobs.New()
}

func TestChunker_Build(t *testing.T) {
	initTest()
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}

	var useCases = []struct {
		filter           map[string]interface{}
		description      string
		caseDataURI      string
		chunkSize        int
		partitionColumns []string
		iDColumns        []string
		pseudoColumns    []*pseudo.Column
		expectCount      int
		expectInSync     int
		expect           interface{}
		partitionStatus  *core.Status
	}{

		{
			description: "insert",
			chunkSize:   5,
			partitionStatus: &core.Status{
				Source: &core.Signature{MinValue: 1, MaxValue: 11, CountValue: 11},
				Dest:   &core.Signature{MinValue: 0, MaxValue: 0, CountValue: 0},
			},
			partitionColumns: []string{"event_type"},
			caseDataURI:      "insert",
			iDColumns:        []string{"id"},
			expectInSync:     0,
			expectCount:      3,

			expect: `{
	"_chunk_00000": "insert",
	"_chunk_00001": "insert",
	"_chunk_00002": "insert"
}`,
		},
		{
			description: "various strategies",
			chunkSize:   5,
			partitionStatus: &core.Status{
				Source: &core.Signature{MinValue: 1, MaxValue: 11, CountValue: 11},
				Dest:   &core.Signature{MinValue: 0, MaxValue: 0, CountValue: 0},
			},
			partitionColumns: []string{"event_type"},
			caseDataURI:      "merge",
			iDColumns:        []string{"id"},
			expectInSync:     0,
			expectCount:      3,

			expect: `{
	"_chunk_00000": "deleteMerge",
	"_chunk_00001": "merge",
	"_chunk_00002": "insert"
}`,
		},
		{
			description: "in sync /append ",
			chunkSize:   5,
			partitionStatus: &core.Status{
				Source: &core.Signature{MinValue: 1, MaxValue: 11, CountValue: 11},
				Dest:   &core.Signature{MinValue: 0, MaxValue: 0, CountValue: 0},
			},
			partitionColumns: []string{"event_type"},
			caseDataURI:      "in_sync",
			iDColumns:        []string{"id"},
			expectInSync:     0,
			expectCount:      1,

			expect: `{"_chunk_00000": "insert"}`,
		},
	}

	ctx := &shared.Context{}

	for _, useCase := range useCases {
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/%v", useCase.caseDataURI)), "", "")
		dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

		dbSync := &contract.Sync{
			Source: &contract.Resource{Table: "events1", Config: chunkerTestConfig},
			Dest:   &contract.Resource{Table: "events2", Config: chunkerTestConfig},
			Table:  "events2",
		}
		dbSync.Source.PseudoColumns = useCase.pseudoColumns
		dbSync.Dest.PseudoColumns = useCase.pseudoColumns
		dbSync.IDColumns = useCase.iDColumns
		dbSync.Chunk.Size = useCase.chunkSize

		err := dbSync.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		daoService := dao.New(dbSync)
		err = daoService.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		partition := core.NewPartition(&dbSync.Strategy, useCase.filter)
		partition.Status = useCase.partitionStatus
		chunker := New(dbSync, partition, daoService, shared.NewMutex(), jobService, transfer.New(dbSync, daoService))
		err = chunker.Build(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		actualInSync := 0
		actual := make(map[string]interface{})
		_ = partition.Chunks.Range(func(chunk *core.Chunk) error {
			if inSync, _ := chunk.InSync(); inSync {
				actualInSync++
				return nil
			}
			actual[chunk.Suffix] = chunk.Status.Method
			return nil
		})
		assert.EqualValues(t, useCase.expectCount, len(actual), useCase.description)
		assert.EqualValues(t, useCase.expectInSync, actualInSync, useCase.description)
		if !assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}
