package scheduler

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestURLToID(t *testing.T) {

	var useCases  = []struct{
		description string
		URL string
		expect string
	} {

		{
			description:"short URL",
			URL:"file:///tmp/abc.txt",
			expect:":tmp:abc.txt",
		},
		{
			description:"lomg URL",
			URL:"file:///data/xxx/22/tmp/abc.txt",
			expect:"22:tmp:abc.txt",
		},
	}

	for _, useCase := range useCases {
		actual := uRLToID(useCase.URL)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}


