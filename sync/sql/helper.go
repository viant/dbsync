package sql

import (
	"github.com/viant/dsc"
	"strings"
)

//normalizeTableName normalizes table name
func normalizeTableName(table string) string {
	for _, aChar := range []string{" ", "-", ":", "\t"} {
		if count := strings.Count(table, aChar); count > 0 {
			table = strings.Replace(table, aChar, "_", count)
		}
	}
	return table
}

//removeTableAliases removes alias from expression
func removeTableAliases(expression, alias string) string {
	count := strings.Count(expression, alias+".")
	if count == 0 {
		return expression
	}
	return strings.Replace(expression, alias+".", "", count)
}

func filterColumns(filter []string, columns []dsc.Column) []dsc.Column {
	if len(filter) == 0 {
		return columns
	}
	filterMap := make(map[string]bool)
	for i := range filter {
		filterMap[strings.ToLower(filter[i])] = true
	}
	var result = make([]dsc.Column, 0)
	for i := range columns {
		if _, ok := filterMap[strings.ToLower(columns[i].Name())]; !ok {
			continue
		}
		result = append(result, columns[i])
	}
	return result
}
