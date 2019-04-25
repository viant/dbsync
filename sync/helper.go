package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"math"
	"time"
)

func waitForSync(syncTaskID int, job *TransferJob) error {
	var response = &TransferResponse{}
	URL := job.StatusURL + fmt.Sprintf("%d", syncTaskID)
	for i := 0; ; i++ {
		job.TimeTaken = time.Now().Sub(job.StartTime)
		err := toolbox.RouteToService("get", URL, nil, response)
		if response != nil && response.WriteCount > 0 {
			job.SetDestCount(response.WriteCount)
		}
		if err != nil || response.Status != "running" {
			break
		}
		if i == 0 {
			time.Sleep(3 * time.Second)
			continue
		}
		time.Sleep(15 * time.Second)
	}
	if response.Status == "error" {
		return NewTransferError(response)
	}
	return nil
}

func indexBy(records []Record, index []string) map[string]Record {
	var result = make(map[string]Record)
	if len(index) == 0 {
		for i, record := range records {
			result[toolbox.AsString(i)] = record
		}
	}
	for _, record := range records {
		key := ""
		for i := range index {
			key += toolbox.AsString(record[index[i]])
		}
		result[key] = record
	}

	return result
}

func cloneMap(source map[string]interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for k, v := range source {
		result[k] = v
	}
	return result
}

func round(value interface{}, numericPrecision int) float64 {
	f := toolbox.AsFloat(value)
	precisionPoint := float64(numericPrecision)
	unit := 1 / math.Pow(10, precisionPoint)
	return math.Round(f/unit) * unit
}

func batchCriteria(partitions []*Partition, diffBatchSize int) []map[string]interface{} {
	if len(partitions) == 0 {
		return nil
	}
	if len(partitions[0].criteriaValues) != 1 {
		return nil
	}
	criteria := make([]map[string]interface{}, 0)
	criterion := []interface{}{}
	var key string
	var value interface{}
	for _, partition := range partitions {
		for key, value = range partition.criteriaValues {
			criterion = append(criterion, value)
			if len(criterion) > diffBatchSize {
				criteria = append(criteria, map[string]interface{}{key: criterion})
				criterion = []interface{}{}
			}
		}
	}
	if len(criterion) > 0 {
		criteria = append(criteria, map[string]interface{}{key: criterion})
	}
	return criteria
}

//IsMapItemEqual compares map item
func IsMapItemEqual(sourceMap, destMap map[string]interface{}, key string) bool {
	if toolbox.IsInt(destMap[key]) || toolbox.IsInt(sourceMap[key]) {
		destMap[key] = toolbox.AsInt(destMap[key])
		sourceMap[key] = toolbox.AsInt(sourceMap[key])
	} else if toolbox.IsFloat(destMap[key]) || toolbox.IsFloat(sourceMap[key]) {
		destMap[key] = toolbox.AsFloat(destMap[key])
		sourceMap[key] = toolbox.AsFloat(sourceMap[key])
	} else if toolbox.IsBool(destMap[key]) || toolbox.IsBool(sourceMap[key]) {
		destMap[key] = toolbox.AsBoolean(destMap[key])
		sourceMap[key] = toolbox.AsBoolean(sourceMap[key])
	}
	return destMap[key] == sourceMap[key]
}