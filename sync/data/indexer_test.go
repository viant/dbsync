package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndexer_Index(t *testing.T) {

	var useCases = []struct {
		description  string
		source       Records
		dest         Records
		keys         []string
		expectSource []string
		expectDest   []string
	}{
		{
			description: "single key index",
			keys:        []string{"k1"},
			source: Records{
				Record{
					"k1": 1,
					"k2": 2,
				},
				Record{
					"k1": 10,
					"k2": 20,
				},
			},
			dest: Records{
				Record{
					"k1": 1,
					"k2": 2,
				},
				Record{
					"k1": 10,
					"k2": 20,
				},
			},
			expectSource: []string{
				"1",
				"10",
			},
		},
		{
			description: "single key index, records count diff",
			keys:        []string{"k1"},
			source: Records{
				Record{
					"k1": 1,
					"k2": 2,
				},
				Record{
					"k1": 10,
					"k2": 20,
				},
				Record{
					"k1": 30,
					"k2": 50,
				},
			},
			dest: Records{
				Record{
					"k1": 1,
					"k2": 2,
				},
				Record{
					"k1": 10,
					"k2": 20,
				},
			},
			expectSource: []string{
				"1",
				"10",
				"30",
			},
			expectDest: []string{
				"1",
				"10",
			},
		},

		{
			description: "single key index, records count diff 2" ,
			keys:        []string{"k1"},
			source: Records{
				Record{
					"k1": 1,
					"k2": 2,
				},
				Record{
					"k1": 10,
					"k2": 20,
				},
			},
			dest: Records{
				Record{
					"k1": 1,
					"k2": 2,
				},
				Record{
					"k1": 10,
					"k2": 20,
				},
				Record{
					"k1": 30,
					"k2": 50,
				},
			},
			expectDest: []string{
				"1",
				"10",
				"30",
			},
			expectSource: []string{
				"1",
				"10",
			},
		},
	}

	for _, useCase := range useCases {

		if len(useCase.expectDest) == 0 {
			useCase.expectDest = useCase.expectSource
		}

		indexer := NewIndexer(useCase.keys)
		index := indexer.Index(useCase.source, useCase.dest)


		assert.Equal(t, len(useCase.expectSource), len(index.Source), useCase.description)
		for _, indexVal := range useCase.expectSource {
			_, hasSource := index.Source[indexVal]
			assert.True(t, hasSource, useCase.description)
		}

		assert.Equal(t, len(useCase.expectDest), len(index.Dest), useCase.description)
		for _, indexVal := range useCase.expectDest {
			_, hasDest := index.Dest[indexVal]
			assert.True(t, hasDest, useCase.description)
		}

	}

}
