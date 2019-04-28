package sync

import "github.com/viant/toolbox"

//Record represents a record
type Record map[string]interface{}

//Records represents records
type Records []Record

//Sum sum data
func (r Records) Sum(column string) int {
	result := 0
	for _, sourceRecord := range r {
		countValue, ok := sourceRecord[column]
		if ok {
			result += toolbox.AsInt(countValue)
		}
	}
	return result
}
