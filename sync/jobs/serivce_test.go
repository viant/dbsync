package jobs

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

func TestService_List(t *testing.T) {

	var useCases = []struct {
		description string
		ids         []string
		startTime   []time.Time

		expect interface{}
	}{
		{
			description: "running job",
			ids:         []string{"1", "2"},
			expect: `{
	"Jobs": [
		{
			"ID": "1",
			"Status": "running"
		},
		{
			"ID": "2",
			"Status": "running"
		}
	]
}`,
		},
		{
			description: "done job",
			ids:         []string{"1", "3", "2"},
			startTime: []time.Time{
				time.Now().Add(-2 * time.Hour),
				time.Now().Add(-time.Hour),
				time.Now().Add(-time.Minute),
			},
			expect: `{
	"Jobs": [
		{
			"ID": "1",
			"Status": "done"
		},
		{
			"ID": "3",
			"Status": "done"
		},
		{
			"ID": "2",
			"Status": "done"
		}
	]
}`,
		},
	}
	now := time.Now()
	for _, useCase := range useCases {
		srv := New()

		for i := range useCase.ids {
			job := srv.Create(useCase.ids[i])
			if i < len(useCase.startTime) {
				job.Done(now)
			}
			assert.EqualValues(t, useCase.ids[i], job.ID, useCase.description)
			response := srv.List(&ListRequest{IDs: []string{useCase.ids[i]}})
			assert.Equal(t, 1, len(response.Jobs))
		}
		actual := srv.List(&ListRequest{})
		if !assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			_ = toolbox.DumpIndent(actual, true)
		}
	}

}
