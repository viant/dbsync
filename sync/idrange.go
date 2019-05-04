package sync

type IdRange struct {
	Min   int
	Max   int
	value int
	delta int
}

func (r *IdRange) Next(inSync bool) int {
	if inSync {
		r.value += r.delta
	} else {
		r.value -= r.delta
	}
	r.delta = int(float64(r.delta) * 0.5)

	return r.value
}

func NewIdRange(min, max int) *IdRange {
	return &IdRange{Min: min, Max: max, value: max, delta: int(float64(max-min) * 0.5)}
}
