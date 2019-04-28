package sync

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_round(t *testing.T) {

	assert.Equal(t, 0.432, round(0.43243434, 3))

}

func Test_IsMapItemEqual(t *testing.T) {

	var r1 = map[string]interface{}{
		"k1": 1,
		"k2": 1,
	}
	var r2 = map[string]interface{}{
		"k1": "1",
		"k2": 12,
	}

	assert.True(t, IsMapItemEqual(r1, r2, "k1"))
	assert.False(t, IsMapItemEqual(r1, r2, "k2"))

}
