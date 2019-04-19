package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

type between struct {
	from int
	to   int
}
type lessOrEqual struct {
	value int
}

type greaterThan struct {
	value int
}

type greaterOrEqual struct {
	value int
}

func toCriterion(k string, v interface{}) string {
	if greaterOrEqual, ok := v.(*greaterOrEqual); ok {
		return fmt.Sprintf("%v >= %v", k, greaterOrEqual.value)
	} else if greaterThan, ok := v.(*greaterThan); ok {
		return fmt.Sprintf("%v > %v", k, greaterThan.value)
	} else if lessOrEqual, ok := v.(*lessOrEqual); ok {
		return fmt.Sprintf("%v <= %v", k, lessOrEqual.value)
	} else if between, ok := v.(*between); ok {
		return fmt.Sprintf("%v BETWEEN %v AND %v", k, between.from, between.to)
	} else if toolbox.IsSlice(v) {
		aSlice := toolbox.AsSlice(v)
		var whereValues = make([]string, 0)
		for _, item := range aSlice {
			if intValue, err := toolbox.ToInt(item); err == nil {
				whereValues = append(whereValues, fmt.Sprintf(`%v`, intValue))
			} else {
				whereValues = append(whereValues, fmt.Sprintf(`'%v'`, item))
			}
		}
		return fmt.Sprintf("%v IN(%v)", k, strings.Join(whereValues, ","))
	} else if _, err := toolbox.ToInt(v); err == nil {
		return fmt.Sprintf("%v = %v", k, v)
	} else {
		return fmt.Sprintf("%v = '%v'", k, v)
	}
}
