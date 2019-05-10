package sync

import (
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

//
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
			dmlType:     DMLInsert,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based insert or replace",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     DMLInsertOrReplace,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based insert on duplicate update",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     DMLInsertOnDuplicateUpddate,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based insert on conflict update",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     DMLInsertOnConflictUpddate,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based merge",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     DMLMerge,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based merge into",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     DMLMergeInto,
			idColumns:   []string{"id"},
		},
		{
			description: "single id based dml delete",
			group:       "id_based",
			suffix:      "_tmp",
			dmlType:     DMLDelete,
			idColumns:   []string{"id"},
		},

		{
			description: "single id and filter based insert or replace",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     DMLInsertOrReplace,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": 4,
			},
		},
		{
			description: "single id and filter based  insert on duplicate update",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     DMLInsertOnDuplicateUpddate,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": " > 4",
			},
		},
		{
			description: "single id and filter based  insert on conflict update",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     DMLInsertOnConflictUpddate,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": " < 4",
			},
		},
		{
			description: "single id and filter based merge",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     DMLMerge,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": &between{1, 10},
			},
		},
		{
			description: "single id and filter based merge into",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     DMLMergeInto,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": &lessOrEqual{1},
			},
		},
		{
			description: "single id and filter based dml delete",
			group:       "filter",
			suffix:      "_tmp",
			dmlType:     DMLDelete,
			idColumns:   []string{"id"},
			filter: map[string]interface{}{
				"event_type": &greaterOrEqual{13},
			},
		},

		{
			description:  "single id based with tempdb insert or replace",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      DMLInsertOrReplace,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb insert on duplicate update",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      DMLInsertOnDuplicateUpddate,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb insert on conflict update",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      DMLInsertOnConflictUpddate,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb merge",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      DMLMerge,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb merge into",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      DMLMergeInto,
			idColumns:    []string{"id"},
		},
		{
			description:  "single id based with tempdb dml delete",
			group:        "tempdb",
			suffix:       "_tmp",
			tempDatabase: "transfer",
			dmlType:      DMLDelete,
			idColumns:    []string{"id"},
		},

		{
			description: "single non id based insert",
			group:       "non_id",
			suffix:      "_tmp",
			dmlType:     DMLInsert,
		},
		{
			description: "single non id based dml delete",
			group:       "non_id",
			suffix:      "_tmp",
			dmlType:     DMLDelete,
		},

		{
			description: "single non id based with filter insert",
			group:       "non_id_filter",
			suffix:      "_tmp",
			dmlType:     DMLInsert,
			filter: map[string]interface{}{
				"event_type": &greaterOrEqual{13},
			},
		},
		{
			description: "single non id based with filter dml delete",
			group:       "non_id_filter",
			suffix:      "_tmp",
			dmlType:     DMLDelete,
			filter: map[string]interface{}{
				"event_type": &greaterOrEqual{13},
			},
		},

		{
			description: "no suffix error",
			group:       "error",
			suffix:      "",
			hasError:    true,
			dmlType:     DMLDelete,
			filter: map[string]interface{}{
				"event_type": &greaterOrEqual{13},
			},
		},
	}

	for _, useCase := range useCases {
		request, err := NewSyncRequestFromURL(baseRequestURL)
		assert.Nil(t, err)
		assert.NotNil(t, request)
		request.Strategy.IDColumns = useCase.idColumns
		if useCase.tempDatabase != "" {
			request.Transfer.TempDatabase = useCase.tempDatabase
		}

		err = request.Init()
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		builder, err := NewBuilder(request, "", false, getTestColumns())
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
			fmt.Printf("DDL %v - %v: \n-------------\n%v\n\n\n\n==============\n", useCase.group, useCase.dmlType, actualSQL)

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
