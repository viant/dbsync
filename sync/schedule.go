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
func (s *Schedule) setNextRun(time time.Time) {
	s.NextRun = &time
}

//IsDue returns true if schedule is due to run
func (s *Schedule) IsDue(baseTime time.Time) bool {
	if s.Disabled {
		return false
	}
	return !baseTime.Before(*s.NextRun)
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
	var nextTime = baseTime
	if s.Frequency != nil {
		duration, _ := s.Frequency.Duration()
		nextTime = baseTime.Add(duration)
	} else if s.At != nil {
		nextTime = s.At.Next(baseTime)
	}
	s.setNextRun(nextTime)
}
