package core

import (
	"dbsync/sync/contract/strategy"
	"dbsync/sync/contract/strategy/diff"
	"dbsync/sync/shared"
	"github.com/viant/toolbox"
	"strings"
)

//Comparator represent record comparator
type Comparator struct {
	*strategy.Diff
	columns map[string]*diff.Column
}

//AreKeysInSync returns true if key of both Source and dest are in sync
func (c *Comparator) AreKeysInSync(ctx *shared.Context, keys []string, record1, record2 Record) bool {
	for _, key := range keys {
		if !c.IsKeyInSync(ctx, key, record1, record2) {
			return false
		}
	}
	return true
}

func (c *Comparator) index() {
	for _, column := range c.Diff.Columns {
		key := strings.ToLower(column.Alias)
		if key == "" {
			key = strings.ToLower(column.Name)
		}
		c.columns[key] = column
	}
}

//IsKeyInSync returns true if key of both Source and dest are in sync
func (c *Comparator) IsKeyInSync(ctx *shared.Context, key string, record1, record2 Record) bool {
	value1, _ := record1.Value(key)
	value2, ok := record2.Value(key)
	if !ok {
		return false
	}
	if value1 == value2 {
		return true
	}
	if c.IsSimilar(key, value1, value2) {
		return true
	}
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
		if !c.IsKeyInSync(ctx, key, record1, record2) {
			return false
		}
	}
	return true
}

//IsSimilar returns true if truncated value1 and value2 are the same
func (c *Comparator) IsSimilar(key string, value1, value2 interface{}) bool {
	c.index()
	if toolbox.IsInt(value1) || toolbox.IsInt(value2) {
		result := toolbox.AsInt(value1) == toolbox.AsInt(value2)
		return result
	}

	column, ok := c.columns[strings.ToLower(key)]
	if !ok {
		return value1 == value2
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
	return value1 == value2
}

//NewComparator creates a new comparator
func NewComparator(strategy *strategy.Diff) *Comparator {
	result := &Comparator{
		Diff:    strategy,
		columns: make(map[string]*diff.Column),
	}
	result.index()
	return result
}
