package sql

import (
	"dbsync/sync/contract"
	"dbsync/sync/criteria"
	"dbsync/sync/method"
	"dbsync/sync/shared"
	"dbsync/sync/sql/diff"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"log"
	"path"
	"reflect"
	"strings"
	"testing"
)

func getTestColumns() []dsc.Column {
	return []dsc.Column{
		dsc.NewColumn("id", "INTEGER", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("ts", "DATETIME", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("event_type", "INTEGER", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("ua", "VARCHAR", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("dnt", "TINYINT", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("charge", "DECIMAL", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("payment", "DECIMAL", nil, nil, nil, reflect.TypeOf(0), nil),
		dsc.NewColumn("modified", "TIMESTAMP", nil, nil, nil, reflect.TypeOf(0), nil),
	}
}

func TestBuilder_DDL(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseRequestURL := path.Join(parent, "test/builder/dif/base_request.yaml")
	request, err := contract.NewRequestFromURL(baseRequestURL)
	if !assert.Nil(t, err) {
		log.Fatal(err)
	}
	_ = request.Init()
	builder, err := NewBuilder(request, "CREATE TABLE events (ID int, name varchar2(255))", getTestColumns())

	{ //basic DDL with suffix
		actualDDL := builder.DDL("_tmp")
		assert.Equal(t, "CREATE TABLE events_tmp (ID int, name varchar2(255))", actualDDL)
	}
	{ //basic DDL
		actualDDL := builder.DDL("")
		assert.Equal(t, "CREATE TABLE events (ID int, name varchar2(255))", actualDDL)
	}
}

func TestBuilder_DDLFromSelect(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseRequestURL := path.Join(parent, "test/builder/dif/base_request.yaml")
	request, err := contract.NewRequestFromURL(baseRequestURL)
	if !assert.Nil(t, err) {
		log.Fatal(err)
	}
	_ = request.Init()
	builder, err := NewBuilder(request, "",  getTestColumns())

	{ //basic DDL with suffix
		actualDDL := builder.DDLFromSelect("_tmp")
		assert.Equal(t, "CREATE TABLE events_tmp AS SELECT * FROM events WHERE 1 = 0", actualDDL)
	}

}

func TestBuilder_ChunkDQL(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseRequestURL := path.Join(parent, "test/builder/chunk/base_request.yaml")
	var useCases = []struct {
		description  string
		suffix       string
		expectDifURI string
		idColumns    []string
		resource     *contract.Resource
		partition    *method.Partition
		max          int
		limit        int
		filter       map[string]interface{}

		hasError bool
	}{
		{
			description:  "default chunk dql",
			idColumns:    []string{"id"},
			suffix:       "_tmp",
			expectDifURI: "default.txt",
			max:          100,
			limit:        20,
		},
		{
			description: "custom chunk dql",
			idColumns:   []string{"id"},
			suffix:      "_tmp",
			resource: &contract.Resource{
				Table: "events",
				ChunkSQL: `SELECT
  MIN(ID) AS MIN_VALUE,
  MAX(ID) AS MAX_VALUE,
  COUNT(1) AS COUNT_VALUE
FROM (
      SELECT   /*+ INDEX_ASC(t PK_ID)*/ ID
      FROM events t
      WHERE ROWNUM <= $limit  $where
) t`,
			},
			expectDifURI: "custom.txt",
			max:          100,
			limit:        20,
		},

		{
			description:  "default chunk with filer dql",
			idColumns:    []string{"id"},
			suffix:       "_tmp",
			expectDifURI: "filter.txt",
			filter: map[string]interface{}{
				"id": " > 30",
			},
			max:   100,
			limit: 20,
		},
		{
			description: "custom chunk with filter dql",
			idColumns:   []string{"id"},
			suffix:      "_tmp",
			resource: &contract.Resource{
				Table: "events",
				ChunkSQL: `SELECT
  MIN(ID) AS MIN_VALUE,
  MAX(ID) AS MAX_VALUE,
  COUNT(1) AS COUNT_VALUE
FROM (
      SELECT   /*+ INDEX_ASC(t PK_ID)*/ ID
      FROM events t
      WHERE ROWNUM <= $limit  $where
) t`,
			},
			expectDifURI: "custom_filter.txt",
			max:          100,
			limit:        20,
			filter: map[string]interface{}{
				"id": " > 30",
			},
		},
	}

	for _, useCase := range useCases {

		request, err := contract.NewRequestFromURL(baseRequestURL)
		if !assert.Nil(t, err) {
			log.Fatal(err)
		}
		if useCase.resource != nil {
			request.Source = useCase.resource
		}
		assert.NotNil(t, request)
		request.IDColumns = useCase.idColumns
		if useCase.partition != nil {
			request.Partition = *useCase.partition
		}
		err = request.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		builder, err := NewBuilder(request, "",  getTestColumns())
		{
			actualDQL, err := builder.ChunkDQL(request.Source, useCase.max, useCase.limit, useCase.filter)
			assert.Nil(t, err, useCase.description)

			expectURL := path.Join(parent, fmt.Sprintf("test/builder/chunk/expect/%v", useCase.expectDifURI))
			expectedDQL, err := loadTextFile(expectURL)
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			if !assert.EqualValues(t, normalizeSQL(expectedDQL), normalizeSQL(actualDQL), useCase.description) {
				fmt.Printf("ChunkDQL %v : \n-------------\n%v\n\n\n\n==============\n", useCase.description, actualDQL)

			}
		}
	}
}

func TestBuilder_CountDQL(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseRequestURL := path.Join(parent, "test/builder/dif/base_request.yaml")
	var useCases = []struct {
		description  string
		suffix       string
		expectDifURI string
		idColumns    []string
		partition    *method.Partition
		resource     *contract.Resource
		diff         *method.Diff
		filter       map[string]interface{}

		hasError bool
	}{
		{
			description:  "single id based with suffix count dql",
			idColumns:    []string{"id"},
			suffix:       "_tmp",
			expectDifURI: "id_based/all.txt",
		},
		{
			description:  "non id based without suffix count  dql",
			suffix:       "",
			expectDifURI: "non_id/all.txt",
		},
	}

	for _, useCase := range useCases {

		request, err := contract.NewRequestFromURL(baseRequestURL)
		if !assert.Nil(t, err) {
			log.Fatal(err)
		}
		assert.NotNil(t, request)
		request.IDColumns = useCase.idColumns
		if useCase.resource != nil {
			request.Source = useCase.resource
		}
		if useCase.diff != nil {
			request.Diff = *useCase.diff
		}
		if useCase.partition != nil {
			request.Partition = *useCase.partition
		}
		err = request.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		builder, err := NewBuilder(request, "",  getTestColumns())
		{
			actualDQL := builder.CountDQL(useCase.suffix, request.Source, useCase.filter)
			expectURL := path.Join(parent, fmt.Sprintf("test/builder/count/expect/%v", useCase.expectDifURI))
			expectedDQL, err := loadTextFile(expectURL)
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			if !assert.EqualValues(t, normalizeSQL(expectedDQL), normalizeSQL(actualDQL), useCase.description) {
				fmt.Printf("CountDQL %v : \n-------------\n%v\n\n\n\n==============\n", useCase.description, actualDQL)

			}
		}
	}
}

func TestBuilder_DiffDQL(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseRequestURL := path.Join(parent, "test/builder/dif/base_request.yaml")
	var useCases = []struct {
		description  string
		expectDifURI string
		idColumns    []string
		partition    *method.Partition
		resource     *contract.Resource
		diff         *method.Diff
		filter       map[string]interface{}

		hasError bool
	}{
		{
			description:  "single id based dql",
			idColumns:    []string{"id"},
			expectDifURI: "id_based/all.txt",
		},
		{
			description:  "count only based dql",
			idColumns:    []string{"id"},
			expectDifURI: "id_based/count_only.txt",
			diff: &method.Diff{
				CountOnly: true,
			},
		},
		{
			description:  "custom based dql",
			idColumns:    []string{"id"},
			expectDifURI: "id_based/custom.txt",
			diff: &method.Diff{
				CountOnly: true,
				Columns: []*diff.Column{
					{
						Name: "charge",
						Func: "MAX",
					},
				},
			},
		},
		{
			description:  "single id based with filter dql",
			idColumns:    []string{"id"},
			expectDifURI: "filter/all.txt",
			filter: map[string]interface{}{
				"id": " > 10",
			},
		},
		{
			description:  "count only based with filter dql",
			idColumns:    []string{"id"},
			expectDifURI: "filter/count_only.txt",
			diff: &method.Diff{
				CountOnly: true,
			},
			filter: map[string]interface{}{
				"id": 10,
			},
		},
		{
			description:  "custom based with filter dql",
			idColumns:    []string{"id"},
			expectDifURI: "filter/custom.txt",
			diff: &method.Diff{
				CountOnly: true,
				Columns: []*diff.Column{
					{
						Name: "charge",
						Func: "MAX",
					},
				},
			},
			filter: map[string]interface{}{
				"id": 15,
			},
		},
		{
			description:  "single id based partition dql",
			idColumns:    []string{"id"},
			expectDifURI: "partition/all.txt",
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
		},
		{
			description:  "single id based position partition dql",
			idColumns:    []string{"id"},
			expectDifURI: "partition/all_position.txt",
			resource: &contract.Resource{
				PositionReference: true,
			},
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
		},
		{
			description:  "non  id based position partition dql",
			expectDifURI: "non_id/all.txt",
			resource: &contract.Resource{
				PositionReference: true,
			},
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
		},
		{
			description:  "non  id based position partition with resource filter dql",
			expectDifURI: "non_id/resource_filter.txt",
			resource: &contract.Resource{
				PositionReference: true,
				Criteria: map[string]interface{}{
					"id": " > 30",
				},
			},
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
		},

		{
			description:  "non  id based position partition with global and resource filter dql",
			expectDifURI: "non_id/mixed_filter.txt",
			resource: &contract.Resource{
				PositionReference: true,
				Criteria: map[string]interface{}{
					"id": " > 30",
				},
			},
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
			filter: map[string]interface{}{
				"event_type": " > 3",
			},
		},
	}

	for _, useCase := range useCases {

		request, err := contract.NewRequestFromURL(baseRequestURL)
		if !assert.Nil(t, err) {
			log.Fatal(err)
		}
		assert.NotNil(t, request)
		request.IDColumns = useCase.idColumns
		if useCase.resource != nil {
			request.Source = useCase.resource
		}
		if useCase.diff != nil {
			request.Diff = *useCase.diff
		}
		if useCase.partition != nil {
			request.Partition = *useCase.partition
		}
		err = request.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		builder, err := NewBuilder(request, "",  getTestColumns())
		{
			actualDQL, _ := builder.DiffDQL(useCase.filter, request.Source)
			expectURL := path.Join(parent, fmt.Sprintf("test/builder/dif/expect/%v", useCase.expectDifURI))
			expectedDQL, err := loadTextFile(expectURL)
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			if !assert.EqualValues(t, normalizeSQL(expectedDQL), normalizeSQL(actualDQL), useCase.description) {
				fmt.Printf("DiffDQL %v : \n-------------\n%v\n\n\n\n==============\n", useCase.description, actualDQL)

			}
		}
	}
}

func TestBuilder_DQL(t *testing.T) {
	parent := toolbox.CallerDirectory(3)
	baseRequestURL := path.Join(parent, "test/builder/dif/base_request.yaml")
	var useCases = []struct {
		description  string
		expectDifURI string
		suffix       string
		idColumns    []string
		partition    *method.Partition
		resource     *contract.Resource
		diff         *method.Diff
		filter       map[string]interface{}

		hasError bool
	}{

		{
			description:  "single id based dql",
			idColumns:    []string{"id"},
			expectDifURI: "id_based/all.txt",
		},
		{
			description:  "non  id based position partition with resource filter dql",
			expectDifURI: "non_id/resource_filter.txt",
			resource: &contract.Resource{
				PositionReference: true,
				Criteria: map[string]interface{}{
					"id": " > 30",
				},
			},
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
		},

		{
			description:  "non  id based position partition with global and resource filter dql",
			expectDifURI: "non_id/mixed_filter.txt",
			resource: &contract.Resource{
				PositionReference: true,
				Criteria: map[string]interface{}{
					"id": " > 30",
				},
			},
			partition: &method.Partition{
				Columns: []string{
					"event_type",
				},
			},
			filter: map[string]interface{}{
				"event_type": " > 3",
			},
		},
	}
	for _, useCase := range useCases {

		request, err := contract.NewRequestFromURL(baseRequestURL)
		if !assert.Nil(t, err) {
			log.Fatal(err)
		}
		assert.NotNil(t, request)
		request.IDColumns = useCase.idColumns
		if useCase.resource != nil {
			request.Source = useCase.resource
		}
		err = request.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		builder, err := NewBuilder(request, "",  getTestColumns())
		{
			actualDQL := builder.DQL(useCase.suffix, request.Source, useCase.filter, len(request.IDColumns) > 0)
			expectURL := path.Join(parent, fmt.Sprintf("test/builder/dql/expect/%v", useCase.expectDifURI))
			expectedDQL, err := loadTextFile(expectURL)
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			if !assert.EqualValues(t, normalizeSQL(expectedDQL), normalizeSQL(actualDQL), useCase.description) {
				fmt.Printf("DQL %v : \n-------------\n%v\n\n\n\n==============\n", useCase.description, actualDQL)

			}
		}
	}
}

func TestBuilder_DML(t *testing.T) {
	parent := toolbox.CallerDirectory(3)

	baseRequestURL := path.Join(parent, "test/builder/dml/base_request.yaml")
	var useCases = []struct {
		group        string
		description  string
		idColumns    []string
		suffix       string
		tempDatabase string
		filter       map[string]interface{}
		dmlType      string
		hasError     bool
	}{

		{
			description: "single id based insert",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsert,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based insert or replace",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsertOrReplace,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based insert on duplicate update",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsertOnDuplicateUpddate,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based insert on conflict update",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsertOnConflictUpddate,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based merge",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLMerge,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based merge into",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLMergeInto,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based dml delete",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.DMLDelete,
			idColumns:   []string{"id"},
		},

		{
			description: "single id based transient delete",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     shared.TransientDMLDelete,
			idColumns:   []string{"id"},
		},

		{
			description: "single id and filter based insert or replace",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsertOrReplace,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": 4,
			},
		},
		{
			description: "single id and filter based  insert on duplicate update",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsertOnDuplicateUpddate,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": " > 4",
			},
		},
		{
			description: "single id and filter based  insert on conflict update",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsertOnConflictUpddate,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": " < 4",
			},
		},
		{
			description: "single id and filter based merge",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLMerge,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": criteria.NewBetween(1, 10),
			},
		},
		{
			description: "single id and filter based merge into",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLMergeInto,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": criteria.NewLessOrEqual(1),
			},
		},
		{
			description: "single id and filter based dml delete",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLDelete,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": criteria.NewGraterOrEqual(13),
			},
		},

		{
			description: "single id based transient delete",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     shared.TransientDMLDelete,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": criteria.NewGraterOrEqual(13),
			},
		},

		{
			description:  "single id based with tempdb insert or replace",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      shared.DMLInsertOrReplace,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb insert on duplicate update",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      shared.DMLInsertOnDuplicateUpddate,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb insert on conflict update",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      shared.DMLInsertOnConflictUpddate,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb merge",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      shared.DMLMerge,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb merge into",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      shared.DMLMergeInto,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb dml delete",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      shared.DMLDelete,
			idColumns:    []string{"id"},
		},

		{
			description: "single non id based insert",
			group:       "non_id",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsert,
		},
		{
			description: "single non id based dml delete",
			group:       "non_id",
			suffix:      "_tmp",
			dmlType:     shared.DMLDelete,
		},

		{
			description: "single non id based with filter insert",
			group:       "non_id_filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLInsert,
			filter: map[string]interface{}{
				"event_type": criteria.NewGraterOrEqual(13),
			},
		},
		{
			description: "single non id based with filter dml delete",
			group:       "non_id_filter",
			suffix:      "_tmp",
			dmlType:     shared.DMLDelete,
			filter: map[string]interface{}{
				"event_type": criteria.NewGraterOrEqual(13),
			},
		},

		{
			description: "no suffix error",
			group:       "error",
			suffix:      "",
			hasError:    true,
			dmlType:     shared.DMLDelete,
			filter: map[string]interface{}{
				"event_type": criteria.NewGraterOrEqual(13),
			},
		},
	}

	for _, useCase := range useCases {
		request, err := contract.NewRequestFromURL(baseRequestURL)
		assert.Nil(t, err)
		assert.NotNil(t, request)
		request.IDColumns = useCase.idColumns
		if useCase.tempDatabase != "" {
			request.Transfer.TempDatabase = useCase.tempDatabase
		}

		err = request.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		builder, err := NewBuilder(request, "",  getTestColumns())
		assert.Nil(t, err)
		assert.NotNil(t, builder)
		actualSQL, err := builder.DML(useCase.dmlType, useCase.suffix, useCase.filter)

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}

		if !assert.Nil(t, err) {
			log.Print(err)
			continue
		}

		expectURL := path.Join(parent, fmt.Sprintf("test/builder/dml/expect/%v/%v.txt", useCase.group, useCase.dmlType))
		expectedSQL, err := loadTextFile(expectURL)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		if !assert.EqualValues(t, normalizeSQL(expectedSQL), normalizeSQL(actualSQL)) {
			fmt.Printf("DML %v - %v: \n-------------\n%v\n\n\n\n==============\n", useCase.group, useCase.dmlType, actualSQL)

		}
	}

}

func TestBuilder_AppendDML(t *testing.T) {
	parent := toolbox.CallerDirectory(3)

	baseRequestURL := path.Join(parent, "test/builder/dml/base_request.yaml")
	var useCases = []struct {
		description  string
		expectDMLURI string
		idColumns    []string
	}{
		{
			description:  "non_id append dml",
			expectDMLURI: "non_id.txt",
		},
		{
			description:  "id based append dml",
			expectDMLURI: "id_based.txt",
			idColumns:    []string{"id"},
		},
	}

	for _, useCase := range useCases {
		request, err := contract.NewRequestFromURL(baseRequestURL)
		assert.Nil(t, err, useCase.description)
		err = request.Init()
		assert.Nil(t, err, useCase.description)
		request.IDColumns = useCase.idColumns
		builder, err := NewBuilder(request, "", getTestColumns())
		assert.Nil(t, err, useCase.description)
		actualSQL := builder.AppendDML("_src1", "_dst2")
		expectURL := path.Join(parent, fmt.Sprintf("test/builder/dml/append/%v", useCase.expectDMLURI))

		expectedSQL, err := loadTextFile(expectURL)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		if !assert.EqualValues(t, normalizeSQL(expectedSQL), normalizeSQL(actualSQL)) {
			fmt.Printf("APPEND shared.DML %v - %v: \n-------------\n%v\n\n\n\n==============\n", useCase.expectDMLURI, useCase.description, actualSQL)

		}

	}
}

func normalizeSQL(text string) string {
	text = strings.Replace(text, "\n", " ", len(text))
	text = strings.Replace(text, "\t", " ", len(text))
	for strings.Contains(text, "  ") {
		text = strings.Replace(text, "  ", " ", len(text))
	}
	text = strings.Replace(text, " ,", ",", len(text))
	text = strings.Replace(text, " )", ")", len(text))
	return strings.TrimSpace(text)
}

func loadTextFile(URL string) (string, error) {
	resource := url.NewResource(URL)
	return resource.DownloadText()
}
