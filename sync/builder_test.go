package sync

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"path"
	"reflect"
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

//
func TestBuilder_DML(t *testing.T) {
	parent := toolbox.CallerDirectory(3)

	var useCases = []struct {
		description   string
		requestURL    string
		columns       map[string]string
		datePartition string
	}{
		{
			description:   "partition based sync",
			requestURL:    path.Join(parent, "test/partition_req.yaml"),
			datePartition: "date(ts) AS date",
		},
		}

	for _, useCase := range useCases {
		request, err := NewSyncRequestFromURL(useCase.requestURL)
		assert.Nil(t, err)
		assert.NotNil(t, request)
		builder, err := NewBuilder(request, "", false, getTestColumns())
		assert.Nil(t, err)
		assert.NotNil(t, builder)
		//TODO add dml use cases
	}

}

//func getPartitionBuilder(t *testing.T) (*Builder, error) {
//	parent := toolbox.CallerDirectory(3)
//	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
//		return nil, fmt.Errorf("unable to init database")
//	}
//	requestURL := path.Join(parent, "test/partition_req.yaml")
//	request, err := NewSyncRequestFromURL(requestURL)
//	assert.Nil(t, err)
//	return NewBuilder(request, nil)
//}
//
//func TestBuilder_DDLBuilder(t *testing.T) {
//	builder, err := getPartitionBuilder(t)
//	if !assert.Nil(t, err) {
//		return
//	}
//	DDL, err := builder.DDL("_tmp")
//	assert.Nil(t, err)
//	expect, _ := loadTextFile("test/expect/ddl.sql")
//	assert.EqualValues(t, normalizeSQL(expect), normalizeSQL(DDL))
//}
//
//func TestBuilder_DQL(t *testing.T) {
//	parent := toolbox.CallerDirectory(3)
//	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
//		return
//	}
//
//	var useCases = []struct {
//		description string
//		requestURL  string
//		values      map[string]interface{}
//		expectURL   string
//	}{
//
//		{
//			description: "partition based sync",
//			values: map[string]interface{}{
//				"date": "2018-01-01",
//				"hour": []interface{}{
//					"2018-01-01 21",
//					"2018-01-01 23",
//				},
//			},
//			requestURL: path.Join(parent, "test/partition_req.yaml"),
//			expectURL:  "test/expect/partition.dql",
//		},
//		{
//			description: "non-partition based sync",
//			requestURL:  path.Join(parent, "test/nonpartition_req.yaml"),
//			expectURL:   "test/expect/nonpartition.dql",
//			values:      map[string]interface{}{},
//		},
//	}
//
//	for _, useCase := range useCases {
//		request, err := NewSyncRequestFromURL(useCase.requestURL)
//		if !assert.Nil(t, err) {
//			continue
//		}
//		assert.Nil(t, request.Init())
//		assert.NotNil(t, request, useCase.description)
//		builder, err := NewBuilder(request, nil)
//
//		DQL := builder.DQL("", request.Source, useCase.values, false)
//		if !assert.Nil(t, err, useCase.description) {
//			continue
//		}
//
//		expect, err := loadTextFile(useCase.expectURL)
//		if !assert.Nil(t, err, useCase.description) {
//			continue
//		}
//		assert.EqualValues(t, normalizeSQL(expect), normalizeSQL(DQL))
//	}
//}
//
//func TestBuilder_CunkDistDQL(t *testing.T) {
//	parent := toolbox.CallerDirectory(3)
//	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
//		return
//	}
//
//	var useCases = []struct {
//		description string
//		requestURL  string
//		values      map[string]interface{}
//		expectURL   string
//	}{
//
//		{
//			description: "partition based sync",
//			values: map[string]interface{}{
//				"date": "2018-01-01",
//				"hour": []interface{}{
//					"2018-01-01 21",
//					"2018-01-01 23",
//				},
//			},
//			requestURL: path.Join(parent, "test/partition_req.yaml"),
//			expectURL:  "test/expect/partition/chunkDist.dql",
//		},
//		{
//			description: "non-partition based sync",
//			requestURL:  path.Join(parent, "test/nonpartition_req.yaml"),
//			expectURL:   "test/expect/nonpartition/chunkDist.dql",
//			values:      map[string]interface{}{},
//		},
//	}
//
//	for _, useCase := range useCases {
//		request, err := NewSyncRequestFromURL(useCase.requestURL)
//		if !assert.Nil(t, err) {
//			continue
//		}
//		assert.Nil(t, request.Init())
//		assert.NotNil(t, request, useCase.description)
//		builder, err := NewBuilder(request, nil)
//		DQL, err := builder.ChunkDQL(request.Source, 0, 1000, useCase.values)
//		fmt.Printf("%v\n", DQL)
//		assert.Nil(t, err)
//		if !assert.Nil(t, err, useCase.description) {
//			continue
//		}
//		expect, err := loadTextFile(useCase.expectURL)
//		if !assert.Nil(t, err, useCase.description) {
//			continue
//		}
//		assert.EqualValues(t, normalizeSQL(expect), normalizeSQL(DQL))
//	}
//}
//
//func TestBuilder_DML(t *testing.T) {
//	parent := toolbox.CallerDirectory(3)
//	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
//		return
//	}
//
//	var useCases = []struct {
//		description string
//		requestURL  string
//		dmlType     string
//		values      map[string]interface{}
//		expectURL   string
//	}{
//
//		{
//			description: "partition based merge",
//			dmlType:     DMLMerge,
//			values: map[string]interface{}{
//				"date": "2018-01-01",
//				"hour": []interface{}{
//					"2018-01-01 21",
//					"2018-01-01 23",
//				},
//			},
//			requestURL: path.Join(parent, "test/partition_req.yaml"),
//			expectURL:  "test/expect/partition/merge.dml",
//		},
//		{
//			description: "non-partition based merge",
//			dmlType:     DMLMerge,
//			requestURL:  path.Join(parent, "test/nonpartition_req.yaml"),
//			expectURL:   "test/expect/nonpartition/merge.dml",
//			values:      map[string]interface{}{},
//		},
//		{
//			description: "partition based insertReplace",
//			dmlType:     DMLInsertOrReplace,
//			values: map[string]interface{}{
//				"date": "2018-01-01",
//				"hour": []interface{}{
//					"2018-01-01 21",
//					"2018-01-01 23",
//				},
//			},
//			requestURL: path.Join(parent, "test/partition_req.yaml"),
//			expectURL:  "test/expect/partition/insertReplace.dml",
//		},
//		{
//			description: "non-partition based insertUpdateOnDuplicate",
//			dmlType:     DMLInsertOnDuplicateUpddate,
//			requestURL:  path.Join(parent, "test/nonpartition_req.yaml"),
//			expectURL:   "test/expect/nonpartition/insertUpdate.dml",
//			values:      map[string]interface{}{},
//		},
//		{
//			description: "non-partition based insert",
//			dmlType:     DMLInsert,
//			requestURL:  path.Join(parent, "test/nonpartition_req.yaml"),
//			expectURL:   "test/expect/nonpartition/insert.dml",
//			values:      map[string]interface{}{},
//		},
//
//		{
//			description: "partition based delete",
//			dmlType:     DMLDelete,
//			values: map[string]interface{}{
//				"date": "2018-01-01",
//				"hour": []interface{}{
//					"2018-01-01 21",
//					"2018-01-01 23",
//				},
//			},
//			requestURL: path.Join(parent, "test/partition_req.yaml"),
//			expectURL:  "test/expect/partition/delete.dml",
//		},
//		{
//			description: "non-partition based delete",
//			dmlType:     DMLDelete,
//			requestURL:  path.Join(parent, "test/nonpartition_req.yaml"),
//			expectURL:   "test/expect/nonpartition/delete.dml",
//			values:      map[string]interface{}{},
//		},
//	}
//
//	for _, useCase := range useCases {
//		request, err := NewSyncRequestFromURL(useCase.requestURL)
//		if !assert.Nil(t, err) {
//			continue
//		}
//		assert.Nil(t, request.Init())
//		assert.NotNil(t, request, useCase.description)
//		builder, err := NewBuilder(request, nil)
//		DML, err := builder.DML("_tmp", useCase.dmlType, useCase.values)
//		if !assert.Nil(t, err, useCase.description) {
//			continue
//		}
//		expect, err := loadTextFile(useCase.expectURL)
//		if !assert.Nil(t, err, useCase.description) {
//			continue
//		}
//		assert.EqualValues(t, normalizeSQL(expect), normalizeSQL(DML))
//	}
//}
//
//func TestBuilder_Diff(t *testing.T) {
//	parent := toolbox.CallerDirectory(3)
//	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
//		return
//	}
//
//	var useCases = []struct {
//		description     string
//		requestURL      string
//		values          map[string]interface{}
//		diffDqlURL      string
//		countDiffDqlURL string
//		dimension       []string
//	}{
//		{
//			description: "partition based diff dql",
//
//			values: map[string]interface{}{
//				"date": "2018-01-01",
//				"hour": []interface{}{
//					"2018-01-01 21",
//					"2018-01-01 23",
//				},
//			},
//			requestURL:      path.Join(parent, "test/partition_req.yaml"),
//			diffDqlURL:      "test/expect/partition/diff.dql",
//			countDiffDqlURL: "test/expect/partition/countDiff.dql",
//			dimension:       []string{"date", "hour"},
//		},
//		{
//			description:     "non-partition based diff dql",
//			requestURL:      path.Join(parent, "test/nonpartition_req.yaml"),
//			diffDqlURL:      "test/expect/nonpartition/diff.dql",
//			countDiffDqlURL: "test/expect/nonpartition/countDiff.dql",
//			values:          map[string]interface{}{},
//		},
//	}
//
//	time, _ := toolbox.ToTime("2018-01-01", toolbox.DateFormatToLayout("yyyy-MM-dd"))
//	var values = map[string]interface{}{
//		"date": time,
//	}
//	for _, useCase := range useCases {
//		request, err := NewSyncRequestFromURL(useCase.requestURL)
//		if !assert.Nil(t, err) {
//			continue
//		}
//		assert.Nil(t, request.Init())
//		assert.NotNil(t, request, useCase.description)
//		builder, err := NewBuilder(request, nil)
//		{
//
//			SQL, dim := builder.DiffDQL(values, builder.dest)
//			sort.Strings(dim)
//			sort.Strings(useCase.dimension)
//			if len(dim) > 0 || len(useCase.dimension) > 0 {
//				assert.EqualValues(t, useCase.dimension, dim)
//			}
//			expect, err := loadTextFile(useCase.diffDqlURL)
//			if !assert.Nil(t, err, useCase.description) {
//				continue
//			}
//			assert.EqualValues(t, normalizeSQL(expect), normalizeSQL(SQL))
//		}
//		{
//			SQL, dim := builder.CountDiffDQL(values, builder.dest)
//			if len(dim) > 0 || len(useCase.dimension) > 0 {
//				assert.EqualValues(t, useCase.dimension, dim)
//			}
//			expect, err := loadTextFile(useCase.countDiffDqlURL)
//			if !assert.Nil(t, err, useCase.description) {
//				continue
//			}
//			assert.EqualValues(t, normalizeSQL(expect), normalizeSQL(SQL))
//		}
//	}
//
//}
//
//func normalizeSQL(text string) string {
//	text = strings.Replace(text, "\n", " ", len(text))
//	text = strings.Replace(text, "\t", " ", len(text))
//	for strings.Contains(text, "  ") {
//		text = strings.Replace(text, "  ", " ", len(text))
//	}
//	text = strings.Replace(text, " ,", ",", len(text))
//	text = strings.Replace(text, " )", ")", len(text))
//	return strings.TrimSpace(text)
//}
//
//func loadTextFile(name string) (string, error) {
//	parent := toolbox.CallerDirectory(3)
//	resource := url.NewResource(path.Join(parent, name))
//	return resource.DownloadText()
//}
