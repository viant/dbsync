package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"math"
	"strings"
	"time"
)

func waitForSync(syncTaskID int, job *TransferJob) error {
	var response = &TransferResponse{}
	URL := job.StatusURL + fmt.Sprintf("%d", syncTaskID)
	for i := 0; ; i++ {
		job.TimeTaken = time.Now().Sub(job.StartTime)
		err := toolbox.RouteToService("get", URL, nil, response)
		if response != nil && response.WriteCount > 0 {
			job.SetTransferredCount(response.WriteCount)
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
			indexValue := getValue(index[i], record)
			key += toolbox.AsString(indexValue)
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

//isMapItemEqual compares map item
func isMapItemsEqual(sourceMap, destMap map[string]interface{}, key []string) bool {
	for _, k := range key {
		if !checkMapItem(sourceMap, destMap, k, func(source, dest interface{}) bool {
			return source == dest
		}) {
			return false
		}
	}
	return len(key) > 0
}

//isMapItemEqual compares map item
func isMapItemEqual(sourceMap, destMap map[string]interface{}, key string) bool {
	return checkMapItem(sourceMap, destMap, key, func(source, dest interface{}) bool {
		return source == dest
	})
}

//isMapItemEqual compares map item
func checkMapItem(sourceMap, destMap map[string]interface{}, key string, check func(source, dest interface{}) bool) bool {
	destValue := getValue(key, destMap)
	sourceValue := getValue(key, sourceMap)
	if toolbox.IsInt(destValue) || toolbox.IsInt(sourceValue) {
		destMap[key] = toolbox.AsInt(destValue)
		sourceMap[key] = toolbox.AsInt(sourceValue)
	} else if toolbox.IsFloat(destValue) || toolbox.IsFloat(sourceValue) {
		destMap[key] = toolbox.AsFloat(destValue)
		sourceMap[key] = toolbox.AsFloat(sourceValue)
	} else if toolbox.IsBool(destMap[key]) || toolbox.IsBool(sourceValue) {
		destMap[key] = toolbox.AsBoolean(destValue)
		sourceMap[key] = toolbox.AsBoolean(sourceValue)
	} else {
		destMap[key] = destValue
		sourceMap[key] = sourceValue
	}
	return check(sourceMap[key], destMap[key])
}

func getValue(key string, data map[string]interface{}) interface{} {
	value, ok := data[key]
	if ok {
		return value
	}
	if !ok {
		for candidate, v := range data {
			if strings.ToLower(candidate) == strings.ToLower(key) {
				return v
			}
		}
	}
	return value
}

func getRecordValue(key string, data map[string]Record) Record {
	value, ok := data[key]
	if ok {
		return value
	}
	if !ok {
		for candidate, v := range data {
			if strings.ToLower(candidate) == strings.ToLower(key) {
				return v
			}
		}
	}
	return value
}

func keyValue(keyValues []string, criteria map[string]interface{}) string {
	var result = make([]string, 0)
	for i := range keyValues {
		value := getValue(keyValues[i], criteria)
		result = append(result, toolbox.AsString(value))
	}
	return strings.Join(result, "_")
}

func removeTableAliases(expression, alias string) string {
	count := strings.Count(expression, alias+".")
	if count == 0 {
		return expression
	}
	return strings.Replace(expression, alias+".", "", count)
}

func isUpperCaseTable(columns []dsc.Column) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if strings.ToLower(column.Name()) == column.Name() {
			return false
		}
	}
	return true
}

func getColumns(manager dsc.Manager, table string) ([]dsc.Column, error) {
	dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
	datastore, err := dialect.GetCurrentDatastore(manager)
	if err != nil {
		return nil, err
	}
	return dialect.GetColumns(manager, datastore, table)
}

func getDDL(manager dsc.Manager, table string) (string, error) {
	dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
	return dialect.ShowCreateTable(manager, table)
}
