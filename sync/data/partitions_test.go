package data

import (
	"dbsync/sync/method"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPartitions_Validate(t *testing.T) {
	var useCases = []struct {
		description string
		strategy    *method.Strategy
		source      Record
		dest        Record
		valid       bool
	}{
		{
			description: "valid partition records",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"group"},
				},
				IDColumns: []string{"id"},
			},
			source:Record{
				"id":1,
				"group":20,
			},
			dest:Record{
				"id":1,
				"group":20,
			},
			valid:true,
		},
		{
			description: "valid partition - no partition columns",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{},
				},
				IDColumns: []string{"id"},
			},
			source:Record{

			},
			dest:Record{

			},
			valid:true,
		},
		{
			description: "invalid partition values",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"group"},
				},
				IDColumns: []string{"id"},
			},
			source:Record{
				"id":1,
				"group":20,
			},
			dest:Record{
				"id":1,
				"group":21,
			},
			valid:false,
		},
	}
	for _, useCase := range useCases {
		comparator := NewComparator(nil, useCase.strategy.Diff.Columns...)
		partitions := NewPartitions([]*Partition{}, useCase.strategy)
		err := partitions.Validate(comparator, useCase.source, useCase.dest)
		if useCase.valid {
			assert.Nil(t, err, useCase.description)
			continue
		}
		assert.NotNil(t, err, useCase.description)

	}
}

func TestPartitions_Range(t *testing.T) {
	var useCases = []struct {
		description      string
		strategy         *method.Strategy
		partitionValues  Records
		errorAfterRepeat int
		expectCount      int
	}{
		{
			description: "range with 4 repeats (single thread)",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1"},
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{"k1": 1},
				Record{"k1": 2},
				Record{"k1": 3},
				Record{"k1": 4},
			},
			expectCount: 4,
		},

		{
			description: "range with 4 repeats (2 threads)",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1"},
					Threads: 2,
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{"k1": 1},
				Record{"k1": 2},
				Record{"k1": 3},
				Record{"k1": 4},
			},
			expectCount: 4,
		},

		{
			description: "error - range with 4 repeats (2 threads)",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1"},
					Threads: 2,
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{"k1": 1},
				Record{"k1": 2},
				Record{"k1": 3},
				Record{"k1": 4},
			},
			errorAfterRepeat: 2,
			expectCount:      4,
		},
	}

	for _, useCase := range useCases {
		var args = make([]*Partition, len(useCase.partitionValues))
		for i := range args {
			args[i] = NewPartition(useCase.strategy, useCase.partitionValues[i])
		}
		partitions := NewPartitions(args, useCase.strategy)

		count := 0
		err := partitions.Range(func(partition *Partition) error {
			count++
			if count == useCase.errorAfterRepeat {
				return fmt.Errorf("test")
			}
			return nil
		})
		if useCase.errorAfterRepeat > 0 {
			assert.NotNil(t, err, useCase.description)
		} else {
			assert.Nil(t, err, useCase.description)
		}
		assert.EqualValues(t, useCase.expectCount, count, useCase.description)
	}
}

func TestPartitions_Init(t *testing.T) {

	var useCases = []struct {
		description     string
		strategy        *method.Strategy
		partitionValues Records
		expectIndex     []string
	}{
		{
			description: "no partition data",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1", "k5", "k3"},
				},
				IDColumns: []string{"id"},
			},
		},
		{
			description: "partition empty data",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1", "k5", "k3"},
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{},
			},
		},
		{
			description: "single key index",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1"},
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{"k1": 1},
				Record{"k1": 2},
			},
			expectIndex: []string{"1", "2"},
		},
		{
			description: "multi key index",
			strategy: &method.Strategy{
				Partition: method.Partition{
					Columns: []string{"k1", "k10"},
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{"k1": 1, "k10": 2, "k": 23},
				Record{"k1": 2},
			},
			expectIndex: []string{"1_2", "2"},
		},
	}

	for _, useCase := range useCases {
		var args = make([]*Partition, len(useCase.partitionValues))
		for i := range args {
			args[i] = NewPartition(useCase.strategy, useCase.partitionValues[i])
		}
		partitions := NewPartitions(args, useCase.strategy)

		partitions.Init()

		assert.EqualValues(t, len(useCase.expectIndex), len(partitions.index), useCase.description)
		for _, indexValue := range useCase.expectIndex {
			partition := partitions.Get(indexValue)
			assert.NotNil(t, partition, useCase.description)
		}
	}
}
