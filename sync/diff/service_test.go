package diff

import (
	"dbsync/sync/dao"
	"dbsync/sync/model"
	"dbsync/sync/shared"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"testing"
)

var testConfig *dsc.Config


func init() {
	parent := toolbox.CallerDirectory(3)
	testConfig = &dsc.Config{
		DriverName: "sqlite3",
		Descriptor: path.Join(parent, "test/db/mydb"),
	}
	_ = testConfig.Init()

}

func TestService_Check(t *testing.T) {

	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test",  "config.yaml")) {
		return
	}

	var useCases = []struct {
		description        string
		caseDataURI        string
		iDColumns          []string
		partitions         []string
		filter map[string]interface{}
		depth              int
		expectInSyncWithID int
		expectMethod       string
		expectInSync       bool
	}{



		{
			description:  "source and dest the same",
			caseDataURI:  "insync",
			iDColumns:    []string{"id"},
			expectInSync: true,
		},
		{
			description:  "dest empty",
			caseDataURI:  "insert",
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodInsert,
		},
		{
			description:  "append data",
			caseDataURI:  "append",
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodInsert,
			expectInSyncWithID:5,
		},


		{
			description:  "merge data",
			caseDataURI:  "merge",
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodMerge,
		},
		{
			description:  "merge data with sync subset",
			caseDataURI:  "merge",
			depth:2,
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodMerge,
			expectInSyncWithID:2,
		},
		{
			description:  "delete with based on min id",
			caseDataURI:  "delete_min",
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodDeleteMerge,
		},
		{
			description:  "delete with based on max id",
			caseDataURI:  "delete_max",
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodDeleteMerge,
		},
		{
			description:  "delete with based on count",
			caseDataURI:  "delete_count",
			iDColumns:    []string{"id"},
			expectMethod:shared.SyncMethodDeleteMerge,
		},

		{
			description:  "delete without id",
			caseDataURI:  "delete_insert",
			iDColumns:    []string{},
			expectMethod:shared.SyncMethodDeleteInsert,
		},

	}


	ctx := &shared.Context{}
	for _, useCase := range useCases {
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/%v", useCase.caseDataURI)), "", "")
		dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

		sync := &model.Sync{
			Source: &model.Resource{Table: "events1", Config: testConfig},
			Dest:   &model.Resource{Table: "events2", Config: testConfig},
			Table:  "events2",
		}
		sync.IDColumns = useCase.iDColumns
		sync.Partition.Columns = useCase.partitions
		sync.Diff.Depth = useCase.depth
		err := sync.Init()
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		service := dao.New(sync)
		err = service.Init(ctx)
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}

		differ := New(sync, service)

		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		source, dest, err := differ.Fetch(ctx, useCase.filter)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		status, err := differ.Check(ctx, source, dest, useCase.filter)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		assert.EqualValues(t, useCase.expectInSync, status.InSync, useCase.description)
		assert.EqualValues(t, useCase.expectMethod, status.Method, useCase.description)
		assert.EqualValues(t, useCase.expectInSyncWithID, status.InSyncWithID, useCase.description)

	}

}

