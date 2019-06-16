package core

import (
	"dbsync/sync/model/strategy"
	"dbsync/sync/shared"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
)

func TestPartitions_Validate(t *testing.T) {

	ctx := &shared.Context{}

	var useCases = []struct {
		description string
		strategy    *strategy.Strategy
		source      Record
		dest        Record
		valid       bool
	}{
		{
			description: "valid partition records",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"group"},
				},
				IDColumns: []string{"id"},
			},
			source: Record{
				"id":    1,
				"group": 20,
			},
			dest: Record{
				"id":    1,
				"group": 20,
			},
			valid: true,
		},
		{
			description: "valid partition - no partition columns",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{},
				},
				IDColumns: []string{"id"},
			},
			source: Record{

			},
			dest: Record{

			},
			valid: true,
		},
		{
			description: "invalid partition Source",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"group"},
				},
				IDColumns: []string{"id"},
			},
			source: Record{
				"id":    1,
				"group": 20,
			},
			dest: Record{
				"id":    1,
				"group": 21,
			},
			valid: false,
		},
	}
	for _, useCase := range useCases {
		comparator := NewComparator(strategy.NewDiff(useCase.strategy.Diff.Columns...))
		partitions := NewPartitions([]*Partition{}, useCase.strategy)
		err := partitions.Validate(ctx, comparator, useCase.source, useCase.dest)
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
		strategy         *strategy.Strategy
		partitionValues  Records
		errorAfterRepeat int
		expectCount      int
	}{
		{
			description: "range with 4 repeats (single thread)",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
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
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
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
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
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
		strategy        *strategy.Strategy
		partitionValues Records
		expectIndex     []string
	}{
		{
			description: "no partition data",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"k1", "k5", "k3"},
				},
				IDColumns: []string{"id"},
			},
		},
		{
			description: "partition empty data",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
					Columns: []string{"k1", "k5", "k3"},
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{},
			},
		},
		{
			description: "single key Index",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
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
			description: "multi key Index",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
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

func TestPartitions_BatchCriteria(t *testing.T) {

	var useCases = []struct {
		description     string
		strategy        *strategy.Strategy
		partitionValues Records
		expect          interface{}
	}{
		{
			description: "single key Index with 2 item batch criteria",
			strategy: &strategy.Strategy{
				Partition: strategy.Partition{
					Columns:   []string{"k1"},
					BatchSize: 2,
				},
				Diff: strategy.Diff{
					BatchSize: 2,
				},
				IDColumns: []string{"id"},
			},
			partitionValues: Records{
				Record{"k1": 1},
				Record{"k1": 2},
				Record{"k1": 3},
			},
			expect: `[
	{
		"k1": [1,2]
	},
	{
		"k1": [3]
	}
]`,
		},
	}

	for _, useCase := range useCases {
		var args = make([]*Partition, len(useCase.partitionValues))
		for i := range args {
			args[i] = NewPartition(useCase.strategy, useCase.partitionValues[i])
		}
		partitions := NewPartitions(args, useCase.strategy)
		partitions.Init()
		criteria := partitions.Criteria()
		if ! assertly.AssertValues(t, useCase.expect, criteria, useCase.description) {
			_ = toolbox.DumpIndent(criteria, true)
		}
	}
}



func TestPartitions_BatchTransferable(t *testing.T) {

	var useCases = []struct {
		description string
		partitions  []*Partition
		expect interface{}
	}{
		{
			description:"inert transferable",
			partitions:[]*Partition{
				{
					IDColumn:"id",
					Transferable: Transferable{
						Suffix:"_tmp1",
						Filter:map[string]interface{}{
							"event_type":1,
						},
						Status: &Status{
							Method:shared.SyncMethodInsert,
							Source:&Signature{CountValue:1},
							Dest:&Signature{CountValue:1},

						},
					},

				},
				{
					IDColumn:"id",
					Transferable: Transferable{
						Suffix:"_tmp2",
						Filter:map[string]interface{}{
							"event_type":2,
						},
						Status: &Status{
							Method:shared.SyncMethodInsert,
							Source:&Signature{CountValue:1},
							Dest:&Signature{CountValue:1},
						},
					},

				},
			},
			expect:`{
	"Method": "insert",
	"Source": {
		"CountValue": 2
	},
	"Suffix": "_tmp"
}
`,

		},
		{
			description:"merge transferable",
			partitions:[]*Partition{
				{
					IDColumn:"id",
					Transferable: Transferable{
						Suffix:"_tmp1",
						Filter:map[string]interface{}{
							"event_type":1,
						},
						Status: &Status{
							Method:shared.SyncMethodMerge,
							Source:&Signature{CountValue:1},
							Dest:&Signature{CountValue:1},

						},
					},

				},
				{
					IDColumn:"id",
					Transferable: Transferable{
						Suffix:"_tmp2",
						Filter:map[string]interface{}{
							"event_type":2,
						},
						Status: &Status{
							Method:shared.SyncMethodInsert,
							Source:&Signature{CountValue:1},
							Dest:&Signature{CountValue:1},
						},
					},

				},
			},
			expect:`{
	"Method": "merge",
	"Source": {
		"CountValue": 2
	},
	"Suffix": "_tmp"
}
`,
		},
	}

	for _, useCase := range useCases {
		partitions := NewPartitions(useCase.partitions, &strategy.Strategy{
			Partition:strategy.Partition{
				BatchSize:10,
			},
		})
		partitions.Init()
		transferable := partitions.BatchTransferable()
		if ! assertly.AssertValues(t, useCase.expect, transferable, useCase.description) {
			_ = toolbox.DumpIndent(transferable, true)
		}
	}

}

