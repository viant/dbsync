package sql

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func Test_NormalizeTableName(t *testing.T) {

	var useCases = []struct {
		description string
		tableName   string
		expect      string
	}{
		{
			description: "whitespace case",
			tableName:   "table 1\t2",
			expect:      "table_1_2",
		},
		{
			description: "separator case",
			tableName:   "table:1 - 2",
			expect:      "table_1___2",
		},
	}

	for _, useCase := range useCases {
		actual := normalizeTableName(useCase.tableName)
		assert.Equal(t, useCase.expect, actual, useCase.description)
	}
}

func Test_RemoveAlias(t *testing.T) {
	var useCases = []struct {
		description string
		expression  string
		alias       string
		expect      string
	}{
		{
			description: "no alias",
			expression:  "SELECT id, name FROM dual",
			expect:      "SELECT id, name FROM dual",
		},
		{
			description: "alias",
			alias:       "t",
			expression:  "SELECT t.id, t.name FROM dual",
			expect:      "SELECT id, name FROM dual",
		},
	}

	for _, useCase := range useCases {
		actual := removeTableAliases(useCase.expression, useCase.alias)
		assert.Equal(t, useCase.expect, actual, useCase.description)
	}
}

func Test_FilterColumns(t *testing.T) {

	var useCases = []struct {
		description string
		columns     []dsc.Column
		filter      []string
		expect      map[string]bool
	}{
		{
			description: "empty filter",
			columns: []dsc.Column{
				dsc.NewSimpleColumn("id", "int"),
				dsc.NewSimpleColumn("name", "varchar"),
				dsc.NewSimpleColumn("updated", "timestamp"),
			},
			expect: map[string]bool{
				"id":      true,
				"name":    true,
				"updated": true,
			},
		},
		{
			description: "filter id,name",
			columns: []dsc.Column{
				dsc.NewSimpleColumn("id", "int"),
				dsc.NewSimpleColumn("name", "varchar"),
				dsc.NewSimpleColumn("updated", "timestamp"),
			},
			filter: []string{"id", "name"},
			expect: map[string]bool{
				"id":   true,
				"name": true,
			},
		},
	}

	for _, useCase := range useCases {
		var actualMap = make(map[string]bool)
		actual := filterColumns(useCase.filter, useCase.columns)
		for i := range actual {
			actualMap[actual[i].Name()] = true
		}
		assert.EqualValues(t, useCase.expect, actualMap, useCase.description)
	}

}
