package core

import (
	"dbsync/sync/model/strategy"
	"dbsync/sync/model/strategy/diff"
	"dbsync/sync/shared"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)



func TestComparator_IsInSync(t *testing.T) {

	var ctx = &shared.Context{}

	var useCases = []struct {
		description string
		columns     []*diff.Column
		record1     Record
		record2     Record
		expect      bool
	}{
		{
			description: "in sync",
			record1: Record{
				"k1": 1,
				"k2": "abc",
			},
			record2: Record{
				"k1": 1,
				"k2": "abc",
			},
			expect: true,
		},
		{
			description: "in sync - truncated",
			record1: Record{
				"k1": 1,
				"k2": "abc",
				"k3": 1.22231,
			},
			record2: Record{
				"k1": 1,
				"k2": "abc",
				"k3": 1.22232,
			},
			columns: []*diff.Column{
				{
					Name:             "k3",
					NumericPrecision: 1,
				},
			},
			expect: true,
		},
		{
			description: "out of sync - truncated",
			record1: Record{
				"k1": 1,
				"k2": "abc",
				"k3": 1.21231,
			},
			record2: Record{
				"k1": 1,
				"k2": "abc",
				"k3": 1.22232,
			},
			columns: []*diff.Column{
				{
					Name:             "k3",
					NumericPrecision: 3,
				},
			},
		},
		{
			description: "out of sync, dest is nil",
			record1: Record{
				"k1": 1,
				"k2": "abc",
			},
		},
		{
			description: "out of sync, Source is nil",
			record2: Record{
				"k1": 1,
				"k2": "abc",
			},
		},
		{
			description: "ouf of sync - key count differs",
			record1: Record{
				"k1": 1,
				"k2": "abc",
				"k3": 3,
			},
			record2: Record{
				"k1": 1,
				"k2": "abc",
			},
		},
		{
			description: "ouf of sync - missing key",
			record1: Record{
				"k1": 1,
				"k2": "abc",
				"k3": 3,
			},
			record2: Record{
				"k1": 1,
				"k2": "abc",
				"k4": 3,
			},
		},
	}

	for _, useCase := range useCases {
		comparator := NewComparator(&strategy.Diff{Columns:useCase.columns})
		actual := comparator.IsInSync(ctx, useCase.record1, useCase.record2)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}

func TestComparator_IsSimilar(t *testing.T) {

	var useCases = []struct {
		description string
		key         string
		column      *diff.Column
		value1      interface{}
		value2      interface{}
		expect      bool
	}{

		{
			description: "similar: time trunc to sec",
			key:         "k1",
			value1:      time.Now(),
			value2:      time.Now().Add(time.Second),
			column:      &diff.Column{Name: "k1", DateLayout: "2006-01-02"},
			expect:      true,
		},
		{
			description: "no similar: no timelayout",
			key:         "k1",
			value1:      time.Now(),
			value2:      time.Now().Add(time.Second),
			column:      &diff.Column{Name: "k1", DateLayout: ""},
			expect:      false,
		},
		{
			description: "no similar: error",
			key:         "k1",
			value1:      "abc",
			value2:      time.Now().Add(time.Second),
			column:      &diff.Column{Name: "k1", DateLayout: "2006-01-02"},
			expect:      false,
		},
		{
			description: "no similar: error",
			key:         "k1",
			value1:      time.Now(),
			value2:      "zz",
			column:      &diff.Column{Name: "k1", DateLayout: "2006-01-02"},
			expect:      false,
		},
		{
			description: "no similar: no matching diff column",
			key:         "k12",
			value1:      time.Now(),
			value2:      "zz",
			column:      &diff.Column{Name: "k1", DateLayout: "2006-01-02"},
			expect:      false,
		},
	}

	for _, useCase := range useCases {
		comparator := NewComparator(&strategy.Diff{Columns:[]*diff.Column{useCase.column}})
		actual := comparator.IsSimilar(useCase.key, useCase.value1, useCase.value2)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}
