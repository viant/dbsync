package sync

import (
	"sync/atomic"
)

//Progress dest to source record transfer progress
type Progress struct {
	DestCount   int64
	SourceCount int
	Pct         int32
}

//SetDestCount sets destination count and compute pct
func (p *Progress) SetDestCount(count int) {
	atomic.StoreInt64(&p.DestCount, int64(count))
	if p.SourceCount > 0 {
		atomic.StoreInt32(&p.Pct, int32(100*count/p.SourceCount))
	}
}
