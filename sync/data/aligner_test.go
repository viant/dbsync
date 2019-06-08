package data

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAlignRecord(t *testing.T) {

	now := time.Now()
	var useCases = []struct {
		description string
		record1     Record
		record2     Record
		expectType  string
		expectKeys  []string
	}{

		{
			description: "int conversion",
			record1: Record{
				"k1": 1,
				"k2": "123",
				"k3": nil,
				"k6": 1,
			},
			record2: Record{
				"k1": "12",
				"k2": 3,
				"k6": 2,
			},
			expectKeys: []string{"k1", "k2"},
			expectType: fmt.Sprintf("%T", int(1)),
		},
		{
			description: "float conversion",
			record1: Record{
				"k1": 1.2,
				"k2": "123",
				"k3": nil,
			},
			record2: Record{
				"k1": "12",
				"k2": 3.3,
			},
			expectKeys: []string{"k1", "k2"},
			expectType: fmt.Sprintf("%T", float64(1.3)),
		},
		{
			description: "boolean conversion",
			record1: Record{
				"k1": true,
				"k2": "123",
				"k3": nil,
			},
			record2: Record{
				"k1": "12",
				"k2": false,
			},
			expectKeys: []string{"k1", "k2"},
			expectType: fmt.Sprintf("%T", false),
		},
		{
			description: "time conversion default timelayout",
			record1: Record{
				"k1": time.Now(),
				"k2": "2019-02-01 01:01:01",
				"k3": nil,
				"k4": time.Now(),
				"k5": time.Now(),
			},
			record2: Record{
				"k1": "2019-01-01 01:01:01",
				"k2": time.Now(),
				"k4": "2019",
				"k5": "abc",
			},
			expectKeys: []string{"k1", "k2", "k4"},
			expectType: fmt.Sprintf("%T", now),
		},
		{
			description: "time conversion RFC3339 timelayout",
			record1: Record{
				"k1": time.Now(),
				"k2": "2019-02-01T01:01:01",
				"k3": nil,
				"k4": time.Now(),
				"k5": time.Now(),
				"k6": "zz",
			},
			record2: Record{
				"k1": "2019-01-01T01:01:01",
				"k2": time.Now(),
				"k4": "2019",
				"k5": "abc",
				"k6": time.Now(),
			},
			expectKeys: []string{"k1", "k2", "k4"},
			expectType: fmt.Sprintf("%T", now),
		},
	}

	for _, useCase := range useCases {

		AlignRecord(useCase.record1, useCase.record2)
		for _, key := range useCase.expectKeys {
			assert.Equal(t, useCase.expectType, fmt.Sprintf("%T", useCase.record1[key]), fmt.Sprintf("source[%v]: %v", key, useCase.description))
			assert.Equal(t, useCase.expectType, fmt.Sprintf("%T", useCase.record2[key]), fmt.Sprintf("dest[%v]: %v", key, useCase.description))
		}
	}

}

func TestAlignRecords(t *testing.T) {

	var useCases = []struct {
		description     string
		records1        Records
		records2        Records
		expectType      string
		expectKeys      []string
		upperAlignIndex int
	}{
		{
			description: "int the same rows conversion",
			records1: Records{Record{
				"k1": 1,
				"k2": "123",
				"k3": nil,
				"k6": 1,
			},
			},
			records2: Records{Record{
				"k1": "12",
				"k2": 3,
				"k6": 2,
			}},
			expectKeys:      []string{"k1", "k2"},
			expectType:      fmt.Sprintf("%T", int(1)),
			upperAlignIndex: 1,
		},
		{
			description: "records1 higher",
			records1: Records{
				Record{
					"k1": 1,
					"k2": "123",
					"k3": nil,
					"k6": 1,
				},
				Record{
					"k1": 2,
					"k2": "123",
					"k3": nil,
					"k6": 1,
				},
				Record{
					"k1": 3,
					"k2": "123",
					"k3": nil,
					"k6": 1,
				},
			},
			records2: Records{
				Record{
					"k1": "12",
					"k2": 3,
					"k6": 2,
				},
				Record{
					"k1": "32",
					"k2": 3,
					"k6": 2,
				},
			},
			expectKeys:      []string{"k1", "k2"},
			expectType:      fmt.Sprintf("%T", int(1)),
			upperAlignIndex: 2,
		},
		{
			description: "records2 higher",
			records1: Records{
				Record{
					"k1": 1,
					"k2": "123",
					"k3": nil,
					"k6": 1,
				},
				Record{
					"k1": 2,
					"k2": "123",
					"k3": nil,
					"k6": 1,
				},
			},
			records2: Records{
				Record{
					"k1": "12",
					"k2": 3,
					"k6": 2,
				},
				Record{
					"k1": "32",
					"k2": 3,
					"k6": 2,
				},
				Record{
					"k1": 3,
					"k2": "123",
					"k3": nil,
					"k6": 1,
				},
			},
			expectKeys:      []string{"k1", "k2"},
			expectType:      fmt.Sprintf("%T", int(1)),
			upperAlignIndex: 2,
		},
	}

	for _, useCase := range useCases {

		AlignRecords(useCase.records1, useCase.records2)

		for i := 0; i < useCase.upperAlignIndex; i++ {
			record1 := useCase.records1[i]
			record2 := useCase.records2[i]
			for _, key := range useCase.expectKeys {
				assert.Equal(t, useCase.expectType, fmt.Sprintf("%T", record1[key]), fmt.Sprintf("source[%v]: %v", key, useCase.description))
				assert.Equal(t, useCase.expectType, fmt.Sprintf("%T", record2[key]), fmt.Sprintf("dest[%v]: %v", key, useCase.description))
			}
		}
	}
}
