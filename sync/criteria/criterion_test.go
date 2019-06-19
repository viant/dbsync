package criteria

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToCriterion(t *testing.T) {

	var useCases = []struct {
		description string
		key         string
		value       interface{}
		expect      string
	}{
		{
			description: "between",
			key:         "k1",
			value:       NewBetween(1, 10),
			expect:      "k1 BETWEEN 1 AND 10",
		},
		{
			description: "less or equal",
			key:         "k1",
			value:       NewLessOrEqual(10),
			expect:      "k1 <= 10",
		},
		{
			description: "grater or equal",
			key:         "k1",
			value:       NewGraterOrEqual(10),
			expect:      "k1 >= 10",
		},
		{
			description: "grater than",
			key:         "k1",
			value:       NewGraterThan(10),
			expect:      "k1 > 10",
		},
		{
			description: "IN with text",
			key:         "k1",
			value:       []string{"v1", "v2"},
			expect:      "k1 IN('v1','v2')",
		},
		{
			description: "IN with number",
			key:         "k1",
			value:       []int{1, 2},
			expect:      "k1 IN(1,2)",
		},
		{
			description: "grater than literal",
			key:         "k1",
			value:       "> 100",
			expect:      "k1 > 100",
		},
		{
			description: "is null literal",
			key:         "k1",
			value:       "is null",
			expect:      "k1 is null",
		},
	}

	for _, useCase := range useCases {

		actual := ToCriterion(useCase.key, useCase.value)
		assert.Equal(t, useCase.expect, actual, useCase.description+fmt.Sprintf("%s", useCase.value))

	}

}
