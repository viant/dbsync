package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecords_Reduce(t *testing.T) {
	var useCases = []struct {
		description string
		records     Records
		key         string
		expectMin   int
		expectSum   int
		expectMax   int
	}{
		{
			description: "empty records",
		},
		{
			description: "records with key value",
			key:         "k1",
			records: Records{
				Record{
					"k1": 1,
				},
				Record{
					"k1": 2,
				},

				Record{
					"k1": 3,
				},
			},
			expectMin: 1,
			expectMax: 3,
			expectSum: 6,
		},
		{
			description: "records with key value reversed",
			key:         "k1",
			records: Records{
				Record{
					"k1": 3,
				},
				Record{
					"k2": 2,
				},
				Record{
					"k1": 1,
				},
			},
			expectMin: 1,
			expectMax: 3,
			expectSum: 4,
		},
		{
			description: "single record with key value",
			key:         "k1",
			records: Records{
				Record{
					"k1": 1,
				},
			},
			expectMin: 1,
			expectMax: 1,
			expectSum: 1,
		},
	}

	for _, useCase := range useCases {
		actualMin := useCase.records.Min(useCase.key)
		actualMax := useCase.records.Max(useCase.key)
		actualSum := useCase.records.Sum(useCase.key)
		assert.EqualValues(t, useCase.expectMin, actualMin, useCase.description)
		assert.EqualValues(t, useCase.expectMax, actualMax, useCase.description)
		assert.EqualValues(t, useCase.expectSum, actualSum, useCase.description)

	}

}
