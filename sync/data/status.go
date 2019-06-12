package data

//Status data signature status
type Status struct {
	Source       *Signature
	Dest         *Signature
	InSync       bool
	isSubset     bool
	InSyncWithID int
	Method       string
}

//Min returns min value
func (s *Status) Min() int {
	if s.Source == nil {
		if s.Dest != nil {
			return s.Dest.Min()
		}
		return 0
	}
	result := s.Source.Min()
	if s.Dest == nil {
		return result
	}
	if s.Dest.Min() > 0 &&  s.Dest.Min() < result || result == 0 {
		result = s.Dest.Min()
	}
	return result
}

//Min returns max value
func (s *Status) Max() int {
	if s.Source == nil {
		if s.Dest != nil {
			return s.Dest.Max()
		}
		return 0
	}
	result := s.Source.Max()
	if s.Dest == nil {
		return result
	}
	if s.Dest.Max() > result {
		result = s.Dest.Max()
	}
	return result
}

func NewStatus(source, dest *Signature) *Status {
	isEqual := source == dest
	if source != nil {
		isEqual = source.IsEqual(dest)
	}
	return &Status{
		Source: source,
		Dest:   dest,
		InSync: isEqual,
	}

}
