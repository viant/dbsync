package history

import (
	"dbsync/sync/core"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRegisterRequest_Validate(t *testing.T) {

	var now = time.Now()

	var useCases = []struct {
		description string
		request     *RegisterRequest
		hasError    bool
	}{

		{
			description:"job was empty",
			request:&RegisterRequest{},
			hasError:true,
		},
		{
			description:"job ID was empty",
			request:&RegisterRequest{
				Job:&core.Job{},
			},
			hasError:true,
		},
		{
			description:"end time was empty",
			request:&RegisterRequest{
				Job:&core.Job{ID:"1"},
			},
			hasError:true,
		},
		{
			description:"valid request",
			request:&RegisterRequest{
				Job:&core.Job{ID:"1",
						EndTime:&now,
					},
			},
		},
	}

	for _, useCase := range useCases {
		err := useCase.request.Validate()
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		assert.Nil(t, err, useCase.description)
	}

}
