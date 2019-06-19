package core

//IDRange represents id range
type IDRange struct {
	Min   int
	Max   int
	value int
	delta int
}

//Next returns next upper bound value
func (r *IDRange) Next(inSync bool) int {
	if inSync {
		r.value += r.delta
	} else {
		r.value -= r.delta
	}
	r.delta = int(float64(r.delta) * 0.5)
	return r.value
}

//NewIDRange create a new id range
func NewIDRange(min, max int) *IDRange {
	return &IDRange{Min: min, Max: max, value: max, delta: int(float64(max-min) * 0.5)}
}
