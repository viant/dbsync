package history

import (
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
)

func TestRegistry_Register(t *testing.T) {

	var useCases = []struct {
		description string
		jobs        []*Job
		expect      interface{}
	}{
		{

			description: "adding jobs",
			jobs: []*Job{
				{
					ID:          "1",
					Status:      "ok",
					Transferred: 10,
				},
				{
					ID:          "1",
					Status:      "ok",
					Transferred: 20,
				},

				{
					ID:          "2",
					Status:      "ok",
					Transferred: 15,
				},
				{
					ID:          "2",
					Status:      "error",
					Error:       "this is test",
					Transferred: 25,
				},
				{
					ID:          "2",
					Status:      "ok",
					Transferred: 35,
				},
				{
					ID:          "3",
					Status:      "ok",
					Transferred: 5,
				},
			},
			expect: `{
	"1": [
		{
			"ID": "1",
			"TimeTakenInMs": 0,
			"Transferred": 20,
			"Status": "ok"
		},
		{
			"ID": "1",
			"Transferred": 10,
			"Status": "ok"
		}
	],
	"2": [
		{
			"ID": "2",
			"TimeTakenInMs": 0,
			"Transferred": 35,
			"Status": "ok"
		},
		{
			"ID": "2",
			"Transferred": 25,
			"Status": "error",
			"Error": "this is test"
		}
	],
	"3": [
		{
			"ID": "3",
			"Transferred": 5,
			"Status": "ok"
		}
	]
}`,
		},
	}

	for _, useCase := range useCases {
		registry := newRegistry(2)
		for i := range useCase.jobs {
			registry.register(useCase.jobs[i])
		}
		actual := registry.list(2)

		if !assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}

	}

}
