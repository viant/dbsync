package core

import (
	"github.com/viant/toolbox"
)

//Records represents records
type Records []Record

//Sum returns sum for supplied key
func (r Records) Sum(key string) int {
	return r.ReduceInt(key, func(prev, next int) int {
		return prev + next
	}, 0)
}

//Max returns max value for supplied key
func (r Records) Max(key string) int {
	return r.ReduceInt(key, func(prev, next int) int {
		if prev > next {
			return prev
		}
		return next
	}, 0)
}

//Signature flatten record into one record where min, max is computed and count is summed
func (r Records) Signature(key string) *Signature {
	if len(r) == 0 {
		return &Signature{}
	}
	result := NewSignatureFromRecord(key, r[0])
	for i := 1; i < len(r); i++ {
		item := NewSignatureFromRecord(key, r[i])
		if item.Min() < result.Min() {
			result.MinValue = item.Min()
		}
		if item.Max() < result.Max() {
			result.MaxValue = item.Max()
		}
		result.CountValue = result.Count() + item.Count()
	}
	return result
}


//Min returns min value for supplied key
func (r Records) Min(key string) int {
	if len(r) == 0 {
		return 0
	}
	return r.ReduceInt(key, func(prev, next int) int {
		if prev < next {
			return prev
		}
		return next
	}, toolbox.AsInt(r[0][key]))
}

//ReduceInt visits each record take value of key and call reducer, if key value is not presents calls with zero
func (r Records) ReduceInt(key string, reducer func(prev, next int) int, initial int) int {
	if len(r) == 0 {
		return initial
	}
	result := initial
	for _, sourceRecord := range r {
		value, ok := sourceRecord.Value(key)
		if ! ok {
			continue
		}
		result = reducer(result, toolbox.AsInt(value))
	}
	return result
}
