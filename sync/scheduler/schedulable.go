package scheduler

import (
	"dbsync/sync/contract"
	"fmt"
	"github.com/viant/toolbox/url"
	"sync/atomic"
	"time"
)

const (
	statusScheduled = iota
	statusRunning
)

//Schedulable represent an abstraction that can be schedule
type Schedulable struct {
	URL string
	ID  string
	*contract.Sync
	Schedule *contract.Schedule
	Status string
	status uint32
}

//Done return true if schedulable is not running
func (s *Schedulable) Done() {
	atomic.StoreUint32(&s.status, statusScheduled)
}

//IsRunning return true if schedulable is running
func (s *Schedulable) IsRunning() bool {
	return atomic.LoadUint32(&s.status) == statusRunning
}

//ScheduleNexRun schedules next run
func (s *Schedulable) ScheduleNexRun(baseTime time.Time) {
	s.Schedule.Next(baseTime)
}

//NewSchedulableFromURL create a new scheduleable from URL
func NewSchedulableFromURL(URL string) (*Schedulable, error) {
	result := &Schedulable{}
	resource := url.NewResource(URL)
	err := resource.Decode(result)
	return result, err
}

//Init initializes scheduleable
func (s *Schedulable) Init() error {
	if s.ID == "" {
		s.ID = uRLToID(s.URL)
	}
	now := time.Now()
	if s.Schedule == nil {
		return nil
	}
	if s.Schedule.Frequency != nil {
		s.Schedule.NextRun = &now
	} else {
		s.Schedule.Next(now)
	}
	return nil

}

//Validate checks if Schedulable is valid
func (s *Schedulable) Validate() error {
	if s.Schedule == nil {
		return fmt.Errorf("schedule was emtpy")
	}
	if s.Schedule.Frequency == nil && s.Schedule.At == nil{
		return fmt.Errorf("schedule.Frequency and schedule.At were emtpy")
	}
	return nil
}
