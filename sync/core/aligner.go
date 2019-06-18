package core

import (
	"github.com/viant/toolbox"
	"reflect"
)

//AlignRecords align records
func AlignRecords(records1, records2 Records) {
	length := len(records1)
	if len(records2) < length {
		length = len(records2)
	}
	for i := 0;i<length;i++{
		AlignRecord(records1[i], records2[i])
	}
}


//AlignRecord align record value data types
func AlignRecord(record1, record2 Record) {
	for key := range record1 {
		value1 := record1[key]
		record2Key := record2.Key(key)
		value2, ok := record2[record2Key]
		if ! ok {
			continue
		}
		if reflect.TypeOf(value1) == reflect.TypeOf(value2) {
			continue
		}
		if toolbox.IsInt(value1) || toolbox.IsInt(value2) {
			record1[key] = toolbox.AsInt(value1)
			record2[key] = toolbox.AsInt(value2)
			continue
		}
		if toolbox.IsTime(value1) || toolbox.IsTime(value2) {
			timeValue1, err := toolbox.ToTime(value1, timeLayout)
			if err == nil {
				timeValue2, err := toolbox.ToTime(value2, timeLayout);
				if err == nil {
					record1[key] = timeValue1.UTC()
					record2[key] = timeValue2.UTC()
				}
			}
			continue
		}
		if toolbox.IsFloat(value1) || toolbox.IsFloat(value2) {
			record1[key] = toolbox.AsFloat(value1)
			record2[key] = toolbox.AsFloat(value2)
			continue
		}
		if toolbox.IsBool(value1) || toolbox.IsBool(value2) {
			record1[key] = toolbox.AsBoolean(value1)
			record2[key] = toolbox.AsBoolean(value2)
		}
	}
}

