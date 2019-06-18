package history

import (
	"dbsync/sync/core"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
	"time"
)


func TestNewJob(t *testing.T) {
	var inAMinute = time.Now().Add(time.Minute)

	var useCases = []struct{
		description string
		job *core.Job
		expect interface{}

	} {
		{
			description:"non chunked job",
			job:&core.Job{
					ID:     "1",
					Status: "ok",
					Items: []*core.Transferable{
						{
							Transferred: 10,
							Status: &core.Status{
								Source:&core.Signature{CountValue:10},
								Dest:&core.Signature{CountValue:5},
							},
						},
					},
					StartTime: time.Now(),
					EndTime:   &inAMinute,
				},
				expect:`{
	"DestCount": 5,
	"ID": "1",
	"Partitions": {
		"inSync": 1
	},
	"SourceCount": 10,
	"Status": "ok",
	"TimeTakenInMs": 59999,
	"Transferred": 10
}
`,
		},
		{
			description:"chunked job",
			job:&core.Job{
				ID:     "1",
				Chunked:true,
				Status: "ok",
				Items: []*core.Transferable{
					{
						Transferred: 10,
						Status: &core.Status{
							Source:&core.Signature{CountValue:10},
							Dest:&core.Signature{CountValue:5},
						},
					},
				},
				StartTime: time.Now(),
				EndTime:   &inAMinute,
			},
			expect:`{
	"DestCount": 5,
	"ID": "1",
	"Chunks": {
		"inSync": 1
	},
	"SourceCount": 10,
	"Status": "ok",
	"TimeTakenInMs": 59999,
	"Transferred": 10
}
`,
		},

	}

	for _, useCase := range useCases {
		job := NewJob(useCase.job)
		if ! assertly.AssertValues(t, useCase.expect, job, useCase.description) {
			_ = toolbox.DumpIndent(job, true)
		}
	}
}