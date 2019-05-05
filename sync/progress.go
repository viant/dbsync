package sync

import (
	"sync/atomic"
)

//Progress dest to source record transfer progress
type Progress struct {
	SourceCount int
	DestCount   int
	Transferred int64
	Pct         int32
}

//SetDestCount sets destination count and compute pct
func (p *Progress) SetTransferredCount(count int) {
	atomic.StoreInt64(&p.Transferred, int64(count))
	if p.SourceCount > 0 {
		atomic.StoreInt32(&p.Pct, int32(100*count/p.SourceCount))
	}
}
