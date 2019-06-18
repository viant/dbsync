package core

//Status data signature status
type Status struct {
	Source       *Signature
	Dest         *Signature
	InSync       bool
	isSubset     bool
	InSyncWithID int
	Method       string
}

//Clone closes status
func (s *Status) Clone() *Status {
	status := *s
	return &status
}

//SetInSyncWithID set filter with min allowed ID
func (s *Status) SetInSyncWithID(ID int) {
	s.InSyncWithID = ID
	skipCount := s.Dest.Count() - (s.Dest.Max() - s.InSyncWithID)
	s.Source.CountValue = s.Source.Count() - skipCount
	s.Dest.CountValue = s.Dest.Count() - skipCount
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

//Max returns max value
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


//NewStatus creates a status
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
