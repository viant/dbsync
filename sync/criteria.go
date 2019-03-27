package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

func criteria(k string, v interface{}) string {
	if toolbox.IsSlice(v) {
		aSlice := toolbox.AsSlice(v)
		var whereValues= make([]string, 0)
		for _, item := range aSlice {
			if toolbox.IsNumber(item) {
				whereValues = append(whereValues, fmt.Sprintf(`%v`, item))
			} else {
				whereValues = append(whereValues, fmt.Sprintf(`'%v'`, item))
			}
		}
		return fmt.Sprintf("%v IN(%v)", k, strings.Join(whereValues, ","))
	} else 	if toolbox.IsNumber(v) {
		return fmt.Sprintf("%v = %v", k, v)
	} else {
		return fmt.Sprintf("%v = '%v'", k, v)
	}
}

