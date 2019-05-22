package sync

import (
	"fmt"
	"github.com/viant/toolbox"
	"time"
)

//Schedule represent schedule meta
type Schedule struct {
	Frequency  *toolbox.Duration
	At         *toolbox.AtTime
	NextRun    *time.Time
	RunCount   int
	ErrorCount int
	Disabled   bool
	SourceURL  string
}

//setNextRun sets next run
func (s *Schedule) setNextRun(ts time.Time) {
	s.NextRun = &ts
}

//IsDue returns true if schedule is due to run
func (s *Schedule) IsDue(baseTime time.Time) bool {
	if s.Disabled {
		return false
	}
	if s.NextRun.Location() != nil {
		baseTime = baseTime.In(s.NextRun.Location())
	}
	return baseTime.After(*s.NextRun) || baseTime.Equal(*s.NextRun)
}

//Validate checks if schedule is valid
func (s *Schedule) Validate() error {
	if s.Frequency == nil && s.At == nil {
		return fmt.Errorf("schedule.Frequency and schedule.At was empty")
	}
	return nil
}

//Schedule schedules next run
func (s *Schedule) Next(baseTime time.Time) {
	baseTime = baseTime.UTC()
	var nextTime = baseTime
	if s.Frequency != nil {
		duration, _ := s.Frequency.Duration()
		nextTime = baseTime.Add(duration)
	} else if s.At != nil {
		nextTime = s.At.Next(baseTime)
	}
	s.setNextRun(nextTime)
}
