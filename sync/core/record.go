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
	result, _ := r.Value(key)
	return result
}


//Key returns key for case insensitive key
func (r Record) Key(key string) string {
	if _, has := r[key]; has {
		return key
	}
	if key == strings.ToUpper(key) {
		lowerKey := strings.ToLower(key)
		if _, has := r[lowerKey]; has {
			return lowerKey
		}
	}
	upperKey := strings.ToUpper(key)
	if _, has := r[upperKey]; has {
		return upperKey
	}
	lowerKey := strings.ToLower(key)
	for candidate := range r {
		if strings.ToLower(candidate) == lowerKey {
			return candidate
		}
	}
	return key
}

//Has return true if case insensitive  key exists
func (r Record) Has(key string) bool {
	_, has := r.Value(key)
	return has
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

//NormIndex returns record Index for the supplied date layout record  keys
func (r Record) NormIndex(dateLayout string, keys []string) string {
	var result = make([]string, 0)
	for i := range keys {
		value, ok := r.Value(keys[i])
		if ! ok {
			continue
		}
		if toolbox.IsTime(value) && dateLayout != "" {
			value = toolbox.AsTime(value, dateLayout).Format(dateLayout)
		}
		result = append(result, toolbox.AsString(value))
	}
	return strings.Join(result, "_")
}

//Index returns record Index for the supplied record  keys
func (r Record) Index(keys []string) string {
	return r.NormIndex("", keys)
}
