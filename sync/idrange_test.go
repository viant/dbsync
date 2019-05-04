package sync

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIdRange(t *testing.T) {

	useCases := []struct {
		description string
		inSync      []bool
		expect      []int
		min         int
		max         int
	}{
		{
			description: "narrowing down",
			inSync:      []bool{false, true, true, true},
			expect:      []int{5000, 7499, 8748, 9372},
			min:         1,
			max:         9999,
		},

		{
			description: "narrowing and backing",
			inSync:      []bool{false, true, true, false},
			expect:      []int{5000, 7499, 8748, 8124},
			min:         1,
			max:         9999,
		},
		{
			description: "narrowing and backing",
			inSync:      []bool{true},
			expect:      []int{14998},
			min:         1,
			max:         9999,
		},
	}
	for _, useCase := range useCases {
		idRange := NewIdRange(useCase.min, useCase.max)
		for i := 0; i < len(useCase.inSync); i++ {
			value := idRange.Next(useCase.inSync[i])
			assert.Equal(t, useCase.expect[i], value, useCase.description+fmt.Sprintf("[%d]", i))
		}
	}
}
