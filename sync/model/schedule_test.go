package model

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

var timeLayout = "2006-01-02 15:04:05"

func TestSchedule_Schedule(t *testing.T) {

	var useCases = []struct {
		description string
		schedule    *Schedule
		baseTime    string
		expectTime  string
	}{
		{
			description: "at: evey minute",
			schedule: &Schedule{At: &toolbox.AtTime{
				WeekDay: "*",
				Hour:    "*",
				Minute:  "*",
			}},
			baseTime:   "2019-01-01 01:01:01",
			expectTime: "2019-01-01 01:02:00",
		},
		{
			description: "at: evey 30 min",
			schedule: &Schedule{At: &toolbox.AtTime{
				WeekDay: "*",
				Hour:    "*",
				Minute:  "0,30",
			}},
			baseTime:   "2019-01-01 01:01:01",
			expectTime: "2019-01-01 01:30:00",
		},
		{
			description: "at: evey 30 min after",
			schedule: &Schedule{At: &toolbox.AtTime{
				WeekDay: "*",
				Hour:    "*",
				Minute:  "0,30",
			}},
			baseTime:   "2019-01-01 01:31:01",
			expectTime: "2019-01-01 02:00:00",
		},
		{
			description: "frequency: every 10 min",
			schedule:    &Schedule{Frequency: &toolbox.Duration{Value: 10, Unit: "min"}},
			baseTime:    "2019-01-01 01:31:01",
			expectTime:  "2019-01-01 01:41:01",
		},
	}
	for _, useCase := range useCases {
		baseTime, err := time.Parse(timeLayout, useCase.baseTime)

		assert.Nil(t, err, useCase.description)
		expectTime, err := time.Parse(timeLayout, useCase.expectTime)
		assert.Nil(t, err, useCase.description)
		useCase.schedule.Next(baseTime)
		assert.Equal(t, expectTime, *useCase.schedule.NextRun, useCase.description)
	}

}

func TestSchedule_Validate(t *testing.T) {
	var useCases = []struct {
		description string
		schedule    *Schedule
		valid       bool
	}{
		{
			description: "valid with at",
			schedule: &Schedule{At: &toolbox.AtTime{
				WeekDay: "*",
				Hour:    "*",
				Minute:  "*",
			}},
			valid: true,
		},
		{
			description: "valid with frequency",
			schedule:    &Schedule{Frequency: &toolbox.Duration{Value: 10, Unit: "min"}},
			valid:       true,
		},
		{
			description: "invalid schedule",
			schedule:    &Schedule{},
		},
	}

	for _, useCase := range useCases {
		err := useCase.schedule.Validate()
		if useCase.valid {
			assert.Nil(t, err, useCase.description)
			continue
		}
		assert.NotNil(t, err, useCase.description)
	}

}

func TestSchedule_IsDue(t *testing.T) {
	var useCases = []struct {
		description string
		schedule    *Schedule
		baseTime    string
		dueBaseTime string
		expect      bool
	}{
		{
			description: "due to run at every minute",
			schedule: &Schedule{At: &toolbox.AtTime{
				WeekDay: "*",
				Hour:    "*",
				Minute:  "*",
			}},
			baseTime:    "2019-01-01 01:01:01",
			dueBaseTime: "2019-01-01 01:02:00",
			expect:      true,
		},
		{
			description: "not due to run at every minute",
			schedule: &Schedule{At: &toolbox.AtTime{
				WeekDay: "*",
				Hour:    "*",
				Minute:  "*",
			}},
			baseTime:    "2019-01-01 01:01:01",
			dueBaseTime: "2019-01-01 01:01:31",
			expect:      false,
		},
		{
			description: "due to run at 10 min frequency",
			schedule:    &Schedule{Frequency: &toolbox.Duration{Value: 10, Unit: "min"}},
			baseTime:    "2019-01-01 01:31:01",
			dueBaseTime: "2019-01-01 01:41:01",
			expect:      true,
		},
		{
			description: "not yet due to run at 10 min frequency",
			schedule:    &Schedule{Frequency: &toolbox.Duration{Value: 10, Unit: "min"}},
			baseTime:    "2019-01-01 01:31:01",
			dueBaseTime: "2019-01-01 01:36:01",
			expect:      false,
		},
	}

	for _, useCase := range useCases {
		baseTime, err := time.Parse(timeLayout, useCase.baseTime)
		assert.Nil(t, err, useCase.description)
		dueBaseTime, err := time.Parse(timeLayout, useCase.dueBaseTime)
		assert.Nil(t, err, useCase.description)
		useCase.schedule.Next(baseTime)
		actual := useCase.schedule.IsDue(dueBaseTime)
		assert.Equal(t, useCase.expect, actual, useCase.description)
	}

}
