package criteria

import (
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
)

func TestBatch_Add(t *testing.T) {

	var useCases = []struct {
		description string
		batchSize   int
		pairs       []map[string]interface{}
		expect      interface{}
	}{
		{
			description: "single key batch",
			batchSize:   2,
			pairs: []map[string]interface{}{
				{
					"k1": 1,
				},
				{
					"k1": 2,
				},
				{
					"k1": 3,
				},
				{
					"k1": 4,
				},
				{
					"k1": 5,
				},
			},
			expect: `[
	{
		"k1": [1,2]
	},
	{
		"k1": [3,4]
	},
	{
		"k1": [5]
	}
]`,
		},

		{
			description: "multi key batch",
			batchSize:   3,
			pairs: []map[string]interface{}{
				{
					"k1": 1,
					"k2": 10,
				},
				{
					"k1": 2,
					"k2": 11,
				},
				{
					"k1": 1,
					"k2": 12,
				},
				{
					"k1": 2,
					"k2": 12,
				},
				{
					"k1": 2,
					"k2": 13,
				},
			},
			expect: `[
	{
		"k1": [1,2],
		"k2": [10,11,12]
	},
	{
		"k1": [2],
		"k2": [12,13]
	}
]
`,
		},
	}

	for _, useCase := range useCases {
		batch := NewBatch(useCase.batchSize)
		for _, pair := range useCase.pairs {
			batch.Add(pair)
		}
		actual := batch.Get()
		if !assertly.AssertValues(t, useCase.expect, actual) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}
