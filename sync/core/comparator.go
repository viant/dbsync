package core

import (
	"dbsync/sync/model/strategy"
	"dbsync/sync/model/strategy/diff"
	"dbsync/sync/shared"
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)


//Comparator represent record comparator
type Comparator struct {
	columns map[string]*diff.Column
}

//AreKeysInSync returns true if key of both Source and dest are in sync
func (c *Comparator) AreKeysInSync(ctx *shared.Context, keys []string, record1, record2 Record) bool {
	for _, key := range keys {
		if ! c.IsKeyInSync(ctx, key, record1, record2) {
			return false
		}
	}
	return true
}

//IsKeyInSync returns true if key of both Source and dest are in sync
func (c *Comparator) IsKeyInSync(ctx *shared.Context, key string, record1, record2 Record) bool {
	value1 := record1[key]
	value2, ok := record2[key]
	if ! ok {
		return false
	}
	if value1 == value2 {
		return true
	}
	if c.IsSimilar(key, value1, value2) {
		return true
	}
	ctx.Log(fmt.Sprintf("difference at %v: %v != %v\n", key, value1, value2))
	return false
}

//IsInSync returns true if Source and dest are in sync
func (c *Comparator) IsInSync(ctx *shared.Context, record1, record2 Record) bool {
	if record1 == nil {
		return record2 == nil 
	}
	if record2 == nil || len(record1) != len(record2) {
		return false
	}
	AlignRecord(record1, record2)
	for key := range record1 {
		if ! c.IsKeyInSync(ctx, key, record1, record2) {
			return false
		}
	}
	return true
}


//IsSimilar returns true if truncated value1 and value2 are the same
func (c *Comparator) IsSimilar(key string, value1, value2 interface{}) bool {
	column, ok := c.columns[strings.ToLower(key)];
	if ! ok {
		return false
	}
	if column.DateLayout != "" {
		timeValue1, err := toolbox.ToTime(value1, column.DateLayout)
		if err != nil {
			return false
		}
		timeValue2, err := toolbox.ToTime(value2, column.DateLayout)
		if err != nil {
			return false
		}
		return timeValue1.Format(column.DateLayout) == timeValue2.Format(column.DateLayout)
	} else if column.NumericPrecision > 0 && round(value1, column.NumericPrecision) == round(value2, column.NumericPrecision) {
			return true
	} 
	return false
}


//NewComparator creates a new comparator
func NewComparator(strategy *strategy.Diff) *Comparator{
	result := &Comparator{
		columns:make(map[string]*diff.Column),
	}
	for _, column := range strategy.Columns {
		result.columns[strings.ToLower(column.Name)] = column
	}
	return result
}