package sync

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInfo_SetMethod(t *testing.T) {

	var useCases = []struct {
		description string
		info        *Info
		method      string
		expect      string
	}{
		{
			description: "empty - insert",
			info:        &Info{},
			method:      SyncMethodInsert,
			expect:      SyncMethodInsert,
		},
		{
			description: "empty - merge",
			info:        &Info{},
			method:      SyncMethodMerge,
			expect:      SyncMethodMerge,
		},
		{
			description: "empty - deletemerge",
			info:        &Info{},
			method:      SyncMethodDeleteMerge,
			expect:      SyncMethodDeleteMerge,
		},
		{
			description: " deletemerge with merge",
			info:        &Info{Method: SyncMethodDeleteMerge},
			method:      SyncMethodMerge,
			expect:      SyncMethodDeleteMerge,
		},
		{
			description: " merge with insert",
			info:        &Info{Method: SyncMethodMerge},
			method:      SyncMethodInsert,
			expect:      SyncMethodMerge,
		},
	}

	for _, useCase := range useCases {
		useCase.info.SetMethod(useCase.method)
		assert.Equal(t, useCase.expect, useCase.info.Method)
	}

}
