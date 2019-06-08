package util

import (
	"github.com/viant/toolbox/url"
	"strings"
)


//URLToID converts URL to ID
func URLToID(URL string) string {
	segments := strings.Split(url.NewResource(URL).ParsedURL.Path, "/")
	if len(segments) > 3 {
		segments = segments[len(segments)-3:]
	}
	pathBasedID := strings.Join(segments, ":")
	pathBasedID = strings.Replace(pathBasedID, "\\", "-", len(pathBasedID))
	pathBasedID = strings.Replace(pathBasedID, " ", "_", len(pathBasedID))
	return pathBasedID
}


//NormalizeTableName normalizes table name
func NormalizeTableName(table string) string {
	for _, achar := range []string{" ", "-", ":"} {
		if count := strings.Count(table, achar); count > 0 {
			table = strings.Replace(table, achar, "_", count)
		}
	}
	return table
}

//RemoveTableAliases removes alias from expression
func RemoveTableAliases(expression, alias string) string {
	count := strings.Count(expression, alias+".")
	if count == 0 {
		return expression
	}
	return strings.Replace(expression, alias+".", "", count)
}