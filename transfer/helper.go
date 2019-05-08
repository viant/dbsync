package transfer

import (
	"strings"
)

func isUpperCaseTable(columns []string) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if strings.ToUpper(column) != column {
			return false
		}
	}
	return true
}

func isLowerCaseTable(columns []string) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if strings.ToLower(column) != column {
			return false
		}
	}
	return true
}
