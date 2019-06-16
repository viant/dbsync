package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStatus_Range(t *testing.T) {

	var useCases =[]struct{
		description string
		source *Signature
		dest *Signature
		expectMin int
		expectMax int

	} {

		{
			description:"dest empty",
			source:&Signature{MaxValue:10, MinValue:3},
			expectMax:10,
			expectMin:3,
		},
		{
			description:"source empty",
			dest:&Signature{MaxValue:10, MinValue:3},
			expectMax:10,
			expectMin:3,
		},
		{
			description:"both empty",
			expectMax:0,
			expectMin:0,
		},
		{
			description:"dest max",
			source:&Signature{MaxValue:5, MinValue:3},
			dest:&Signature{MaxValue:10, MinValue:3},
			expectMin:3,
			expectMax:10,
		},
		{
			description:"dest min",
			source:&Signature{MaxValue:5, MinValue:3},
			dest:&Signature{MaxValue:10, MinValue:2},
			expectMin:2,
			expectMax:10,
		},
		{
			description:"source min zero",
			source:&Signature{MaxValue:5, MinValue:0},
			dest:&Signature{MaxValue:10, MinValue:2},
			expectMin:2,
			expectMax:10,
		},
		{
			description:"dest min zero",
			source:&Signature{MaxValue:5, MinValue:3},
			dest:&Signature{MaxValue:10, MinValue:0},
			expectMin:3,
			expectMax:10,
		},
	}


	for _, useCase := range useCases {

		status := NewStatus(useCase.source, useCase.dest)

		assert.Equal(t, useCase.expectMin, status.Min(), useCase.description)
		assert.Equal(t, useCase.expectMax, status.Max(), useCase.description)

	}

}