package core

import (
	"dbsync/sync/contract/strategy"
	"dbsync/sync/shared"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
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
			expectSuffix: "_tmp99_20190101010101",
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
			expectSuffix: "_tmp99_20190101010101",
			expectID:     "id",
		},
	}

	for _, useCase := range useCases {
		useCase.partition.Init()
		assert.Equal(t, useCase.expectSuffix, useCase.partition.Suffix)
		assert.Equal(t, useCase.expectID, useCase.partition.IDColumn)
	}

}



func TestPartition_BatchCriteria(t *testing.T) {


	partitionStretagy :=&strategy.Strategy{
		Partition: strategy.Partition{},
		Chunk:strategy.Chunk{Threads:10,},
	}
	var useCases = []struct {
		description     string
		chunks []*Chunk
		expect          interface{}
	}{
		{
			description: "batch merge criteria",
			chunks:[]*Chunk{
				{
					Transferable: Transferable{
						Suffix:"_tmp1_001",
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
					Transferable: Transferable{
						Suffix:"_tmp1_002",
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
			},
			expect:`{
	"Method": "merge",
	"Source": {
		"CountValue": 2
	},
	"Suffix": "_tmp"
}`,
		},

		{
			description: "batch insert criteria",
			chunks:[]*Chunk{
				{
					Transferable: Transferable{
						Suffix:"_tmp1_001",
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
					Transferable: Transferable{
						Suffix:"_tmp1_002",
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
			},
			expect:`{
	"Method": "insert",
	"Source": {
		"CountValue": 2
	},
	"Suffix": "_tmp"
}`,
		},
	}

	for _, useCase := range useCases {


		partitions := NewPartition(partitionStretagy, map[string]interface{}{})
		partitions.Init()
		for i := range useCase.chunks {
			partitions.Chunks.Offer(useCase.chunks[i])
		}

		actual := partitions.BatchTransferable()
		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}
}
