package transfer

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
)

func TestTransfer(t *testing.T) {
	transfers := newTransfers(&Request{WriterThreads: 3, BatchSize: 2})
	var index = 0
	for i := 0; i < 6; i++ { //push data across 3 writer thread batches
		err := transfers.push(map[string]interface{}{
			"id": index,
		})
		index++
		assert.Nil(t, err)
	}
	assert.EqualValues(t, 3, transfers.index)
	for i := 0; i < 3; i++ {
		assert.EqualValues(t, 2, transfers.transfers[i].count)
		assert.EqualValues(t, 0, transfers.transfers[i].collection.Size())
	}

	var expectedIndex = 0
	for i := 0; i < 3; i++ {
		batch := transfers.transfers[i].getBatch()
		assert.EqualValues(t, 2, batch.size)
		err := batch.ranger.Range(func(item interface{}) (bool, error) {
			record := toolbox.AsMap(item)
			assert.EqualValues(t, expectedIndex, record["id"])
			expectedIndex++
			return true, nil
		})
		assert.Nil(t, err)
	}

}
