package criteria

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"testing"
)

func TestBatchMap_Range(t *testing.T) {

	aMap := NewBatchMap(2)

	for i := 0; i < 11; i++ {
		aMap.Add(fmt.Sprintf("k%d", i%3), map[string]interface{}{"k1": i})
	}
	actual := map[string][]map[string]interface{}{}
	_ = aMap.Range(func(key string, batch *Batch) error {
		actual[key] = batch.Get()

		return nil
	})

	assert.Equal(t, 3, len(actual))

	expect := `{
	"k0": [
			{
				"k1": [0, 3]
			},
			{
				"k1": [6, 9]
			}
		],
		"k1": [
			{
				"k1": [1, 4]
			},
			{
				"k1": [7, 10]
			}
		],
	"k2": [
			{
				"k1": [2, 5]
			},
			{
				"k1": [8]
			}
		]
}`

	assertly.AssertValues(t, expect, actual)
}
