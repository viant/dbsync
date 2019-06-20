package scheduler

import (
	"dbsync/sync/contract"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
)

func TestSchedulable_Validate(t *testing.T) {

	var useCases = []struct {
		description string
		schedulable *Schedulable
		hasError    bool
	}{
		{
			description: "valid schduleable",

			schedulable: &Schedulable{
				ID: "123",
				Schedule: &contract.Schedule{
					Frequency: &toolbox.Duration{
						Unit:  "minute",
						Value: 1,
					},
				},
			},
		},
		{
			description: "id is empty error",
			hasError:    true,
			schedulable: &Schedulable{
				Schedule: &contract.Schedule{
					Frequency: &toolbox.Duration{
						Unit:  "minute",
						Value: 1,
					},
				},
			},
		},
		{
			description: "valid schduleable",

			schedulable: &Schedulable{
				ID: "123",
				Schedule: &contract.Schedule{
					Frequency: &toolbox.Duration{
						Unit:  "minute",
						Value: 1,
					},
				},
			},
		},
		{
			description: "empty schedule",
			hasError:    true,
			schedulable: &Schedulable{
				ID:       "123",
				Schedule: &contract.Schedule{},
			},
		},
		{
			description: "empty schedule",
			hasError:    true,
			schedulable: &Schedulable{
				ID: "123",
			},
		},
	}

	for _, useCase := range useCases {
		err := useCase.schedulable.Validate()
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		assert.Nil(t, err, useCase.description)
	}

}
