package sql

import "strings"

//normalizeTableName normalizes table name
func normalizeTableName(table string) string {
	for _, aChar := range []string{" ", "-", ":"} {
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
