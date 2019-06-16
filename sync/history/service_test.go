package history

import (
	"dbsync/sync/core"
	"dbsync/sync/shared"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

func TestService_Show(t *testing.T) {

	var inAMinute = time.Now().Add(time.Minute)
	var useCases = []struct {
		description string
		maxHistroy  int
		ID          string
		jobs        []*core.Job
		expect      interface{}
	}{
		{
			description: "max history 1",
			maxHistroy:  1,
			ID:          "1",
			jobs: []*core.Job{
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 10,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 15,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},

				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "error",
					Error:  "test error",
					Items: []*core.Transferable{
						{
							Transferred: 3,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Error:  "",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},

					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
			},
			expect: `{
	"Items": [
		{
			"ID": "1",
			"Status": "ok",
			"TimeTakenInMs": 59999
		}
	]
}`,
		},

		{
			description: "max history 2",
			maxHistroy:  2,
			ID:          "2",
			jobs: []*core.Job{
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 10,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 15,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},

				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "error",
					Error:  "test error",
					Items: []*core.Transferable{
						{
							Transferred: 3,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Error:  "",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},

					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
			},
			expect: `{
	"Items": [
		{
			"ID": "2",
			"Status": "ok",
			"TimeTakenInMs": 59999
		},
        {
			"ID": "2",
			"Status": "error",
			"TimeTakenInMs": 59999
		}
	]
}`,
		},
	}

	for _, useCase := range useCases {

		service := New(&shared.Config{MaxHistory: useCase.maxHistroy})
		for _, job := range useCase.jobs {
			service.Register(job)
		}
		actual := service.Show(&ShowRequest{
			ID: useCase.ID,
		})

		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}

	}
}

func TestService_Status(t *testing.T) {

	var inAMinute = time.Now().Add(time.Minute)
	var useCases = []struct {
		description string
		maxHistroy  int
		runCount int
		ID          string
		jobs        []*core.Job
		expect      interface{}
	}{
		{
			description: "status ok",
			maxHistroy:  3,
			runCount:0,
			ID:          "1",
			jobs: []*core.Job{
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 10,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 15,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},

				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Error:  "",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},

					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
			},
			expect: `{
	"Status": "ok",
	"Transferred": {
		"1": 15,
		"2": 5
	}
}`,
		},
		{
			description: "status error",
			maxHistroy:  3,
			runCount:2,
			ID:          "1",
			jobs: []*core.Job{
				{
					ID:     "1",
					Status: "error",
					Error:  "test error",
					Items: []*core.Transferable{
						{
							Transferred: 10,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 15,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},

				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				{
					ID:     "2",
					Status: "ok",
					Error:  "",
					Items: []*core.Transferable{
						{
							Transferred: 5,
						},
					},

					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
			},
			expect: `{
	"Error": "test error",
	"Errors": {
		"1": "test error"
	},
	"Status": "error",
	"Transferred": {
		"1": 15,
		"2": 5
	}
}`,
		},
	}

	for _, useCase := range useCases {

		service := New(&shared.Config{MaxHistory: useCase.maxHistroy})
		for _, job := range useCase.jobs {
			service.Register(job)
		}
		actual := service.Status(&StatusRequest{
			RunCount: useCase.runCount,
		})

		if ! assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}

	}
}
