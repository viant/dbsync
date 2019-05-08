package sync

import "fmt"
import "github.com/viant/toolbox"

type differ struct {
	*Builder
}

func (d *differ) IsEqual(index []string, source, dest []Record, status *Info) bool {
	indexedSource := indexBy(source, index)
	indexedDest := indexBy(dest, index)
	for key := range indexedSource {
		sourceRecord := indexedSource[key]
		destRecord := getRecordValue(key, indexedDest)
		if destRecord == nil {
			return false
		}
		discrepant := false
		for k, sourceValue := range sourceRecord {
			if !checkMapItem(sourceRecord, destRecord, k, func(source, dest interface{}) bool {
				return source == dest
			}) {
				destValue := getValue(k, destRecord)
				fmt.Printf("discrep :%v %v %T %T\n", destValue, sourceValue, destValue, sourceValue)
				discrepant = true
				break
			}
		}

		if discrepant { //Try apply date format or numeric rounding to compare again
			for _, column := range d.Builder.Diff.Columns {
				key := column.Alias
				if !isMapItemEqual(destRecord, sourceRecord, key) {
					destValue := getValue(key, destRecord)
					sourceValue := getValue(key, sourceRecord)
					if column.DateLayout != "" {
						destTime, err := toolbox.ToTime(destValue, column.DateLayout)
						if err != nil {
							return false
						}
						sourceTime, err := toolbox.ToTime(sourceValue, column.DateLayout)
						if err != nil {
							return false
						}
						if destTime.Format(column.DateLayout) != sourceTime.Format(column.DateLayout) {
							return false
						}
					} else if column.NumericPrecision > 0 {
						if round(destValue, column.NumericPrecision) != round(sourceValue, column.NumericPrecision) {
							return false
						}
					} else {
						return false
					}
				}
			}
		}
	}
	return true

}
