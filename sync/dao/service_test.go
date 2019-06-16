package dao

import (
	"dbsync/sync/criteria"
	"dbsync/sync/model"
	"dbsync/sync/shared"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
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


func TestService_Builder(t *testing.T) {
	sync := &model.Sync{
		Source: &model.Resource{
			Config: testConfig,
		},
		Dest: &model.Resource{
			Config: testConfig,
		},
		Table: "events",
	}
	err := sync.Init()
	assert.Nil(t, err)

	service := New(sync)
	_ = service.Init(&shared.Context{})
	assert.NotNil(t, service.Builder())

}



func TestService_Init(t *testing.T) {
	sync := &model.Sync{
		Source: &model.Resource{
			Config: testConfig,
		},
		Dest: &model.Resource{
			Config: testConfig,
		},
		Table: "z z",
	}
	err := sync.Init()
	assert.Nil(t, err)

	service := New(sync)
	err = service.Init(&shared.Context{})
	assert.NotNil(t, err)

}

func TestService_Partitions(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}

	ctx := &shared.Context{}
	initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, "test/data/partition"), "", "")
	dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

	var useCases = []struct {
		description string
		resource    *model.Resource
		expect      interface{}
		hasError    bool
	}{
		{
			description: "single key partition",
			resource: &model.Resource{
				PartitionSQL: "SELECT event_type FROM events GROUP BY 1 ORDER BY 1",
				Config:       testConfig,
			},
			expect: `[
	{
		"event_type": 2
	},
	{
		"event_type": 3
	},
	{
		"event_type": 4
	}
]`,
		},
		{
			description: "multi key partition",
			resource: &model.Resource{

				PartitionSQL: "SELECT DATE(timestamp) AS date , event_type FROM events WHERE event_type > 3 GROUP BY 1, 2 ORDER BY 1 DESC, 2",
				Config:       testConfig,
			},
			expect: `[
	{
		"date": "2019-04-10",
		"event_type": 4
	},
	{
		"date": "2019-03-18",
		"event_type": 4
	},
	{
		"date": "2019-03-13",
		"event_type": 4
	}
]`,
		},
		{
			description: "error SQL",
			resource: &model.Resource{
				PartitionSQL: "SELECT event_type FROM wrong_table GROUP BY 1 ORDER BY 1",
				Config:       testConfig,
			},
			hasError: true,
		},
	}

	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: useCase.resource,
			Dest:   useCase.resource,
		}
		service := New(sync)
		err := service.initDB(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		defer service.Close()
		actual, err := service.Partitions(ctx, model.ResourceKindDest)

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}

func TestService_Columns(t *testing.T) {
	var useCases = []struct {
		description string
		table       string
		resource    *model.Resource
		expect      interface{}
		hasError    bool
	}{
		{
			description: "events colums",
			table:       "events",
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `{
	"event_type": "INTEGER",
	"id": "INTEGER",
	"modified": "TIMESTAMP",
	"quantity": "DECIMAL(7, 2)",
	"timestamp": "DATETIME"
}`},

		{
			description: "invalid table name",
			table:       "events '  _abc",
			resource: &model.Resource{
				Config: testConfig,
			},
			hasError: true,
		},
	}
	ctx := &shared.Context{}

	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: useCase.resource,
			Dest:   useCase.resource,
		}
		service := New(sync)
		err := service.initDB(ctx)
		assert.Nil(t, err)
		columns, err := service.Columns(ctx, useCase.table)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		var actual = map[string]string{}
		for _, column := range columns {
			actual[column.Name()] = column.DatabaseTypeName()
		}

		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}
}

func TestService_Signatures(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}
	initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, "test/data/signatures"), "", "")
	dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

	var useCases = []struct {
		description string
		ID          []string
		partitions  []string
		resource    *model.Resource
		table       string
		expect      interface{}
		hasError    bool
	}{
		{
			description: "single signature without id",
			table:       "events",
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `[
	{
		"cnt": 11,
		"cnt_quantity": 11,
		"sum_event_type": 32,
		"sum_id": 66
	}
]`},

		{
			description: "single signature with id",
			ID:          []string{"id"},
			table:       "events",
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `[
	{
		"cnt": 11,
		"cnt_quantity": 11,
		"max_id": 11,
		"min_id": 1,
		"non_null_cnt_id": 11,
		"sum_event_type": 32,
		"sum_id": 66,
		"unique_cnt_id": 11
	}
]`},


		{
			description: "single signature with id and partition",
			ID:          []string{"id"},
			table:       "events",
			partitions:  []string{"event_type"},
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `[
	{
		"@indexBy@":"event_type"
	},
	{
		"cnt": 4,
		"cnt_quantity": 4,
		"event_type": 2
	},
	{
		"cnt": 4,
		"cnt_quantity": 4,
		"event_type": 3
	},
	{
		"cnt": 3,
		"cnt_quantity": 3,
		"event_type": 4
	}
]`},
		{
			description: "invalid partition column",
			ID:          []string{"id"},
			table:       "events",
			partitions:  []string{"event_worng_type"},
			resource: &model.Resource{
				Config: testConfig,
			},
			hasError: true,
		},
	}

	ctx := &shared.Context{}
	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: useCase.resource,
			Dest:   useCase.resource,
			Table:  useCase.table,
		}
		sync.IDColumns = useCase.ID
		sync.Partition.Columns = useCase.partitions
		err := sync.Init()
		assert.Nil(t, err)
		service := New(sync)
		err = service.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		actual, err := service.Signatures(ctx, model.ResourceKindSource, nil)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}
}


func TestService_Signature(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}
	initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, "test/data/signatures"), "", "")
	dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

	var useCases = []struct {
		description string
		ID          []string
		partitions  []string
		resource    *model.Resource
		table       string
		expect      interface{}
		hasError    bool
	}{
		{
			description: "single signature without id",
			table:       "events",
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `{
		"cnt": 11,
		"cnt_quantity": 11,
		"sum_event_type": 32,
		"sum_id": 66
	}`},

		{
			description: "single signature with id",
			ID:          []string{"id"},
			table:       "events",
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `
	{
		"cnt": 11,
		"cnt_quantity": 11,
		"max_id": 11,
		"min_id": 1,
		"non_null_cnt_id": 11,
		"sum_event_type": 32,
		"sum_id": 66,
		"unique_cnt_id": 11
	}
`},


		{
			description: "invalid partition column",
			ID:          []string{"id"},
			table:       "events",
			partitions:  []string{"event_worng_type"},
			resource: &model.Resource{
				Config: testConfig,
			},
			hasError: true,
		},
		{
			description: "single signature with id and partition",
			ID:          []string{"id"},
			table:       "events",
			partitions:  []string{"event_type"},
			resource: &model.Resource{
				Config: testConfig,
			},
			hasError:true,
		},
	}

	ctx := &shared.Context{Debug:false,}
	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: useCase.resource,
			Dest:   useCase.resource,
			Table:  useCase.table,
		}
		sync.IDColumns = useCase.ID
		sync.Partition.Columns = useCase.partitions
		err := sync.Init()
		assert.Nil(t, err)
		service := New(sync)
		err = service.Init(ctx)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		actual, err := service.Signature(ctx, model.ResourceKindSource, nil)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}



func TestService_CountSignature(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}
	initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, "test/data/count_signature"), "", "")
	dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

	var useCases = []struct {
		description string
		ID          []string
		filter      map[string]interface{}
		resource    *model.Resource
		table       string
		expect      interface{}
		hasError    bool
	}{
		{
			description: "single signature without id",
			table:       "events",
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `{
		"CountValue": 11
	}`},
		{
			description: "single signature with id",
			table:       "events",
			ID:          []string{"id"},
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `{
	"CountValue": 11,
	"MaxValue": 11,
	"MinValue": 1
}`},
		{
			description: "single signature with id",
			table:       "events",
			ID:          []string{"id"},
			resource: &model.Resource{
				Config: testConfig,
			},
			filter: map[string]interface{}{
				"id": criteria.NewBetween(4, 20),
			},
			expect: `{
	"CountValue": 8,
	"MaxValue": 11,
	"MinValue": 4
}`},
	}

	ctx := &shared.Context{}

	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: useCase.resource,
			Dest:   useCase.resource,
			Table:  useCase.table,
		}
		sync.IDColumns = useCase.ID
		err := sync.Init()
		assert.Nil(t, err)
		service := New(sync)
		err = service.Init(ctx)
		assert.Nil(t, err)

		signature, err := service.CountSignature(ctx, model.ResourceKindDest, useCase.filter)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		if ! assertly.AssertValues(t, useCase.expect, signature, useCase.description) {
			_ = toolbox.DumpIndent(signature, true)
		}
	}

}





func TestService_ChunkSignature(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}
	initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, "test/data/count_signature"), "", "")
	dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset))

	var useCases = []struct {
		description string
		ID          []string
		filter      map[string]interface{}
		resource    *model.Resource
		table       string
		offset      int
		limit       int

		expect   interface{}
		hasError bool
	}{
		{
			description: "single signature with id",
			table:       "events",
			ID:          []string{"id"},
			offset:      5,
			limit:       2,
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `{
	"CountValue": 2,
	"MaxValue": 6,
	"MinValue": 5
}`},
		{
			description: "single signature with id with filter",
			table:       "events",
			ID:          []string{"id"},
			offset:      5,
			limit:       2,
			filter: map[string]interface{}{
				"event_type": "> 2",
			},
			resource: &model.Resource{
				Config: testConfig,
			},
			expect: `{
	"CountValue": 2,
	"MaxValue": 6,
	"MinValue": 5
}`},
	}

	ctx := &shared.Context{}
	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: useCase.resource,
			Dest:   useCase.resource,
			Table:  useCase.table,
		}
		sync.IDColumns = useCase.ID
		err := sync.Init()
		assert.Nil(t, err)
		service := New(sync)
		err = service.Init(ctx)
		assert.Nil(t, err)

		signature, err := service.ChunkSignature(ctx, model.ResourceKindDest, useCase.offset, useCase.limit, useCase.filter)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		if ! assertly.AssertValues(t, useCase.expect, signature, useCase.description) {
			_ = toolbox.DumpIndent(signature, true)
		}
	}

}

func TestService_RecreateTransientTable(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {

		return
	}

	var useCases = []struct {
		description string
		table       string
		suffix      string
		tempdb      string
		hasError    bool
	}{

		{
			description:"create as select",
			table:"events",
			suffix:"_tmp",
		},

		{
			description:"error - empty suffix",
			table:"events",
			suffix:"",
			hasError:true,
		},
		{
			description:"create as select",
			table:"events",
			tempdb:"tmp",
			suffix:"_tmp1",
			hasError:true,
		},

	}

	ctx := &shared.Context{Debug:true}
	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: &model.Resource{
				Config: testConfig,
			},
			Dest: &model.Resource{
				Config: testConfig,
			},
			Table: useCase.table,
		}
		sync.Transfer.TempDatabase = useCase.tempdb
		err := sync.Init()
		assert.Nil(t, err)

		service := New(sync)
		err = service.Init(ctx)
		assert.Nil(t, err)
		err = service.RecreateTransientTable(ctx, useCase.suffix)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}


	}

}



func TestService_DropTransientTable(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {

		return
	}

	var useCases = []struct {
		description string
		table       string
		createSuffix string
		removeSuffix      string
		tempdb      string
		hasError    bool
	}{

		{
			description:"create as select",
			table:"events",
			createSuffix:"_tmp",
			removeSuffix:"_tmp",
		},

		{
			description:"error - empty suffix",
			table:"events",
			createSuffix:"_tmp",
			removeSuffix:"",
			hasError:true,
		},

		{
			description:"error - invalid suffix",
			table:"events",
			createSuffix:"_tmp",
			removeSuffix:"_abc",
			hasError:true,
		},
		{
			description:"error - invalid suffix with tempdb",
			table:"events",
			tempdb:"abc",
			createSuffix:"_tmp",
			removeSuffix:"_abc",
			hasError:true,
		},


	}

	ctx := &shared.Context{Debug:true}
	for _, useCase := range useCases {
		sync := &model.Sync{
			Source: &model.Resource{
				Config: testConfig,
			},
			Dest: &model.Resource{
				Config: testConfig,
			},
			Table: useCase.table,

		}
		sync.Transfer.TempDatabase = useCase.tempdb
		err := sync.Init()
		assert.Nil(t, err)

		service := New(sync)
		err = service.Init(ctx)
		assert.Nil(t, err)
		_ = service.CreateTransientTable(ctx, useCase.createSuffix)
		err = service.DropTransientTable(ctx, useCase.removeSuffix)

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if ! assert.Nil(t, err, useCase.description) {
			continue
		}


	}

}
