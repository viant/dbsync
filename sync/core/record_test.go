package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecord_Index(t *testing.T) {
	var useCases = []struct {
		description string
		record      Record
		keys        []string
		expect      string
	}{
		{
			description: "single key Index",
			record: Record{
				"k1": 1,
				"k2": 2,
				"k6": 1,
			},
			keys:   []string{"k1"},
			expect: "1",
		},
		{
			description: "multi keys Index",
			record: Record{
				"k1": 1,
				"k2": 2,
				"k3": 3,
				"k6": 22,
			},
			keys:   []string{"k1", "k2"},
			expect: "1_2",
		},
		{
			description: "multi keys Index with gap",
			record: Record{
				"k1": 1,
				"k3": 3,
				"k6": 22,
			},
			keys:   []string{"k1", "k2", "k3"},
			expect: "1_3",
		},
	}

	for _, useCase := range useCases {
		actual := useCase.record.Index(useCase.keys)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}
}

func TestRecord_Value(t *testing.T) {
	var useCases = []struct {
		description string
		record      Record
		key         string
		expect      interface{}
		has         bool
	}{
		{
			description: "exact key value",
			record: Record{
				"abc": 1,
			},
			key:    "abc",
			expect: 1,
			has:    true,
		},
		{
			description: "mix case key value",
			record: Record{
				"aBc": 3,
			},
			key:    "abc",
			expect: 3,
			has:    true,
		},
		{
			description: "no key value",
			record: Record{
				"aBc": 3,
			},
			key: "k1",
		},
	}

	for _, useCase := range useCases {
		actual, has := useCase.record.Value(useCase.key)
		if !useCase.has {
			assert.False(t, has, useCase.description)
			continue
		}
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}
}
