package transfer

import (
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"sync/atomic"
	"time"
)

type transfer struct {
	closed     int32
	batchSize  uint64
	collection *data.CompactedSlice
	isFlushed  chan bool
	batches    chan *transferBatch
	count      uint64
}

func (t *transfer) push(record map[string]interface{}) error {
	if t.isClosed() {
		return fmt.Errorf("transfer is closed")
	}
	t.collection.Add(record)
	result := atomic.AddUint64(&t.count, 1)
	if result%t.batchSize == 0 {
		t.batches <- newBatch(t.collection)
	}
	return nil
}

func (t *transfer) isClosed() bool {
	return atomic.LoadInt32(&t.closed) == 1
}

func (t *transfer) flush() {
	select {
	case t.isFlushed <- true:
	case <-time.After(time.Millisecond):
	}
}

func (t *transfer) close() {
	atomic.StoreInt32(&t.closed, 1)
	t.flush()
}

func (t *transfer) getBatch() *transferBatch {
	if t.isClosed() {
		select {
		case b := <-t.batches:
			return b
		case <-time.After(time.Millisecond):
		}
		return newBatch(t.collection)
	}

	select {
	case b := <-t.batches:
		return b
	case <-t.isFlushed:
		return newBatch(t.collection)
	}
}

func newTransfer(request *TransferRequest) *transfer {
	return &transfer{
		batchSize:  uint64(request.BatchSize),
		collection: data.NewCompactedSlice(request.OmitEmpty, true),
		batches:    make(chan *transferBatch, 1),
		isFlushed:  make(chan bool, 1),
	}
}

type transfers struct {
	transfers []*transfer
	index     uint64
	batchSize int
	count     uint64
}

func (t *transfers) push(record map[string]interface{}) error {
	var index = int(atomic.LoadUint64(&t.index)) % len(t.transfers)
	count := int(atomic.AddUint64(&t.count, 1))
	if count%t.batchSize == 0 {
		index = (int(atomic.AddUint64(&t.index, 1)) - 1) % len(t.transfers)
	}
	return t.transfers[index].push(record)
}

func (t *transfers) close() {
	for _, transfer := range t.transfers {
		transfer.close()
	}
}

func (t *transfers) flush() {
	for _, transfer := range t.transfers {
		transfer.flush()
	}
}

func newTransfers(request *TransferRequest) *transfers {
	if request.WriterThreads == 0 {
		request.WriterThreads = 1
	}
	if request.BatchSize == 0 {
		request.BatchSize = 1
	}
	var result = &transfers{
		batchSize: request.BatchSize,
		transfers: make([]*transfer, request.WriterThreads),
	}
	for i := 0; i < request.WriterThreads; i++ {
		result.transfers[i] = newTransfer(request)
	}
	return result
}

type transferBatch struct {
	fields []*data.Field
	size   int
	ranger toolbox.Ranger
}

func newBatch(collection *data.CompactedSlice) *transferBatch {
	size := collection.Size()
	return &transferBatch{
		size:   size,
		ranger: collection.Ranger(),
		fields: collection.Fields(),
	}
}
