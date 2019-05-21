package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

//Criteria represents filter values
type Criteria map[string]interface{}

type between struct {
	from int
	to   int
}

func (b between) String() string {
	return fmt.Sprintf("BETWEEN %v AND %v", b.from, b.to)
}

type lessOrEqual struct {
	value int
}

func (c lessOrEqual) String() string {
	return fmt.Sprintf(" >= %v", c.value)
}

type greaterThan struct {
	value int
}

func (c greaterThan) String() string {
	return fmt.Sprintf(" < %v", c.value)
}

type greaterOrEqual struct {
	value int
}

func (c greaterOrEqual) String() string {
	return fmt.Sprintf(" =< %v", c.value)
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
				itemLiteral := toolbox.AsString(item)
				if strings.HasPrefix(itemLiteral, "(") && strings.HasSuffix(itemLiteral, ")") {
					whereValues = append(whereValues, fmt.Sprintf(`%v`, item))
				} else {
					whereValues = append(whereValues, fmt.Sprintf(`'%v'`, item))
				}
			}
		}
		return fmt.Sprintf("%v IN(%v)", k, strings.Join(whereValues, ","))
	} else if _, err := toolbox.ToInt(v); err == nil {
		return fmt.Sprintf("%v = %v", k, v)
	} else {
		literal := strings.TrimSpace(toolbox.AsString(v))
		lowerLiteral := strings.ToLower(literal)
		if strings.Contains(literal, ">") ||
			strings.Contains(literal, "<") ||
			strings.Contains(lowerLiteral, " null") {
			return fmt.Sprintf("%v %v", k, v)
		}
		return fmt.Sprintf("%v = '%v'", k, v)
	}
}

func batchCriteria(partitions []*Partition, diffBatchSize int) []map[string]interface{} {
	if len(partitions) == 0 {
		return nil
	}
	batch := newCriteriaBatch(diffBatchSize)
	for _, partition := range partitions {

		for key, value := range partition.criteria {
			if batch.hasValue(value) {
				continue
			}
			batch.append(key, value)
		}
	}
	batch.flush()
	return batch.criteria
}

func updateBatchedPartitions(session *Session) {
	partitions := session.Partitions.data
	batchSize := session.Request.Partition.BatchSize
	if len(partitions) == 0 {
		return
	}
	batch := newCriteriaBatch(batchSize)
	for _, partition := range partitions {
		if partition.Info == nil {
			session.Error(partition, "partition with no sync info - giving up batching")
			return
		}
		if partition.InSync {
			continue
		}
		session.Log(partition, fmt.Sprintf("sync method: %v, %v",partition.Info.Method, partition.criteria))
		batch.info.DestCount += partition.Info.DestCount
		batch.info.SourceCount += partition.Info.SourceCount
		batch.info.SetMethod(partition.Info.Method)
		for key, value := range partition.criteria {
			if batch.hasValue(value) {
				continue
			}
			batch.append(key, value)
		}
	}
	batch.flush()
	var result = make([]*Partition, 0)
	for i := range batch.criteria {
		partition := NewPartition(session.Request.Partition, batch.criteria[i], session.Request.Chunk.Threads, session.Request.IDColumns[0])
		partition.SetInfo(batch.statuses[i])
		partition.Suffix = fmt.Sprintf("_tmp%v", i)
		session.Log(partition, fmt.Sprintf("final sync method: %v, %v",partition.Info.Method, partition.criteria))
		if partition.Method == SyncMethodInsert {
			partition.Method = SyncMethodMerge
		}
		result = append(result, partition)
	}
	session.Partitions = NewPartitions(result, session)
}

type criteriaBatch struct {
	batchSize    int
	criteria     []map[string]interface{}
	values       map[string][]interface{}
	uniqueValues map[interface{}]bool
	size         int
	info         *Info
	statuses     []*Info
}

func (b *criteriaBatch) append(key string, value interface{}) {
	if _, ok := b.values[key]; !ok {
		b.values[key] = make([]interface{}, 0)
	}
	b.values[key] = append(b.values[key], value)
	b.size++
	if b.size >= b.batchSize {
		b.flush()
	}
}

func (b *criteriaBatch) flush() {
	if b.size == 0 {
		return
	}
	var criterion = make(map[string]interface{})
	for k, v := range b.values {
		criterion[k] = v
	}
	b.criteria = append(b.criteria, criterion)
	b.statuses = append(b.statuses, b.info)
	b.size = 0
	b.info = &Info{}
	b.uniqueValues = make(map[interface{}]bool)
	b.values = make(map[string][]interface{})
}

func (b *criteriaBatch) hasValue(value interface{}) bool {
	if _, ok := b.uniqueValues[value]; ok {
		return ok
	}
	b.uniqueValues[value] = true
	return false
}

func newCriteriaBatch(batchSize int) *criteriaBatch {
	return &criteriaBatch{
		batchSize:    batchSize,
		criteria:     make([]map[string]interface{}, 0),
		values:       make(map[string][]interface{}),
		uniqueValues: make(map[interface{}]bool),
		statuses:     make([]*Info, 0),
		info:         &Info{},
	}
}
