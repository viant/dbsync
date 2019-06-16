package merge

import (
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/data"
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

func TestService_Sync(t *testing.T) {

	parent := toolbox.CallerDirectory(3)

	var useCases = []struct {
		description  string
		caseDataURI  string
		iDColumns    []string
		appendOnly   bool
		transferable *core.Transferable
		hasError     bool
	}{

		{
			description: "append only new data, ",
			caseDataURI: "append_only",
			appendOnly:  true,
			iDColumns:   []string{"id"},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodInsert,
			},
		},


		{
			description: "append only new data, remove duplicated",
			caseDataURI: "append",

			iDColumns: []string{"id"},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodInsert,
			},
		},


		{
			description: "merge data",
			caseDataURI: "merge",

			iDColumns: []string{"id"},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodMerge,
			},
		},
		{
			description: "delete merge with filter",
			caseDataURI: "delete_merge",

			iDColumns: []string{"id"},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodDeleteMerge,
				Filter: map[string]interface{}{"id": ">=5"},
			},
		},

		{
			description: "delete insert with filter",
			caseDataURI: "delete_insert",

			iDColumns: []string{"id"},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodDeleteInsert,
				Filter: map[string]interface{}{"id": ">=5"},
			},
		},

		{
			description: "delete insert without id and filter",
			caseDataURI: "delete_insert_noid_filter",

			iDColumns: []string{},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodDeleteInsert,
				Filter: map[string]interface{}{"id": ">=5"},
			},
		},
		{
			description: "delete insert without id ",
			caseDataURI: "delete_insert_noid",

			iDColumns: []string{},
			transferable: &core.Transferable{
				Suffix: "_tmp",
				Method: shared.SyncMethodDeleteInsert,
				Filter: map[string]interface{}{"id": ">=5"},
			},
		},

		{
			description: "error - no suffix",
			caseDataURI: "append_only",
			appendOnly:  true,
			iDColumns:   []string{"id"},
			transferable: &core.Transferable{
				Suffix: "",
				Method: shared.SyncMethodDeleteMerge,
			},
			hasError:true,
		},
		{
			description: "error - direct",
			caseDataURI: "append_only",
			appendOnly:  true,
			iDColumns:   []string{"id"},
			transferable: &core.Transferable{
				IsDirect:true,
				Suffix: "",
				Method: shared.SyncMethodDeleteMerge,
			},
			hasError:true,
		},
		{
			description: "error - unknown method",
			caseDataURI: "append_only",
			iDColumns:   []string{"id"},
			transferable: &core.Transferable{
				Suffix: "",
				Method: "blah blah",
			},
			hasError:true,
		},

	}

	ctx := &shared.Context{Debug: true}
	for _, useCase := range useCases {
		if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/%v/prepare", useCase.caseDataURI)), "", "")
		dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

		sync := &model.Sync{
			Source: &model.Resource{Table: "events1", Config: testConfig},
			Dest:   &model.Resource{Table: "events2", Config: testConfig},
			Table:  "events2",
		}
		sync.IDColumns = useCase.iDColumns
		sync.AppendOnly = useCase.appendOnly

		err := sync.Init()
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		service := dao.New(sync)
		err = service.Init(ctx)
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}

		merger := New(sync, service, shared.NewMutex())

		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		err = merger.Merge(ctx, useCase.transferable)

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		expectDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/data/%v/expect", useCase.caseDataURI)), "", "")
		dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.FullTableDatasetCheckPolicy, expectDataset))

	}

}
