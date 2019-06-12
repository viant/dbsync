package data

import (
	"dbsync/sync/model/strategy"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPartition_Init(t *testing.T) {
	time, _ := time.Parse(timeLayout, "2019-01-01 01:01:01")
	var useCases = []struct {
		description  string
		partition    *Partition
		expectSuffix string
		expectID     string
	}{
		{
			description: "default partition",
			partition: NewPartition(&strategy.Strategy{
				Partition: strategy.Partition{},
			}, Record{
				"k1": 1, "k2": 2,
			}),
			expectSuffix: "_tmp",
		},
		{
			description: "single column partition",
			partition: NewPartition(&strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"k1"},
				},
			}, Record{
				"k1": 1, "k2": 2,
			}),
			expectSuffix: "_tmp1",
		},
		{
			description: "single column partition with ID",
			partition: NewPartition(&strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"k1"},
				},
				IDColumns: []string{"id"},
			}, Record{
				"k1": 1, "k2": 2,
			}),
			expectSuffix: "_tmp1",
			expectID:     "id",
		},
		{
			description: "multi column partition with ID",
			partition: NewPartition(&strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"k1", "k3"},
				},
				IDColumns: []string{"id"},
			}, Record{
				"k1": 99, "k2": 2, "k3": time,
			}),
			expectSuffix: "_tmp9920190101010101",
			expectID:     "id",
		},
		{
			description: "multi column partition with ID and missing Source",
			partition: NewPartition(&strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"k1", "k5", "k3"},
				},
				IDColumns: []string{"id"},
			}, Record{
				"k1": 99, "k2": 2, "k3": time,
			}),
			expectSuffix: "_tmp9920190101010101",
			expectID:     "id",
		},
	}

	for _, useCase := range useCases {
		useCase.partition.Init()
		assert.Equal(t, useCase.expectSuffix, useCase.partition.Suffix)
		assert.Equal(t, useCase.expectID, useCase.partition.IDColumn)
	}

}
