package sync

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_round(t *testing.T) {

	assert.Equal(t, 0.432, round(0.43243434, 3))

}
