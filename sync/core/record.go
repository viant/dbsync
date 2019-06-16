package core

import (
	"github.com/viant/toolbox"
	"strings"
)

const timeLayout = "2006-01-02 15:04:05"

//Record represents a record
type Record map[string]interface{}


//Get returns records value or nil
func (r Record) Get(key string) interface{} {
	return r[key]
}

//Value returns a value if key, using firsy map lookup then case insensitive strategy.
func (r Record) Value(key string) (interface{}, bool) {
	value, ok := r[key]
	if ok {
		return value, ok
	}
	lowerKey := strings.ToLower(key)
	for candidate, v := range r {
		if strings.ToLower(candidate) == lowerKey {
			return v, true
		}
	}
	return nil, false
}

//Index returns record Index for the supplied record  keys
func (r Record) Index(keys []string) string {
	var result = make([]string, 0)
	for i := range keys {
		value, ok := r.Value(keys[i])
		if ! ok {
			continue
		}
		result = append(result, toolbox.AsString(value))
	}
	return strings.Join(result, "_")
}
