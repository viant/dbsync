package scheduler

import (
	"dbsync/sync/shared"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"github.com/viant/toolbox/url"
	"log"
	"path"
	"strings"
	"sync"
	"time"
)

var defaultSchedulerLoadFrequencyMs = 5000
var dateLayout = "2006-01-02 03:04:05"

//Service represents a scheduler
type Service interface {
	//List returns list of scheduled jobs
	List(request *ListRequest) *ListResponse
	//Get returns requested scheduled job
	Get(ID string) *Schedulable
}

//Runner represents runner function
type Runner func(schedulable *Schedulable) error

//service represents basic scheduler
type service struct {
	*shared.Config
	refreshDuration time.Duration
	schedules       map[string]*Schedulable
	modified        map[string]time.Time
	mutex           *sync.Mutex
	nextCheck       time.Time
	runner          Runner
}

//add adds runnable
func (s *service) add(schedulable *Schedulable, modTime time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, has := s.schedules[schedulable.ID]
	if !has {
		log.Printf("Added schedule: %v\n", schedulable.ID)
	} else {
		log.Printf("Updated schedule: %v\n", schedulable.ID)
	}
	s.schedules[schedulable.ID] = schedulable
	s.modified[schedulable.ID] = modTime
}

func (s *service) List(request *ListRequest) *ListResponse {
	response := &ListResponse{
		Items: s.getSchedulables(),
	}
	return response
}

//listIDs lists runnable IDs
func (s *service) listIDs() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var result = make([]string, 0)
	for k := range s.schedules {
		result = append(result, k)
	}
	return result
}

//Get returns runnable by ID
func (s *service) Get(ID string) *Schedulable {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.schedules[ID]
}

//hasChanged returns runnable by ID
func (s *service) hasChanged(ID string, modTime time.Time) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	timeValue, ok := s.modified[ID]
	if !ok {
		return true
	}
	return !timeValue.Equal(modTime)
}

//remove remove runnable by ID
func (s *service) remove(ID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.schedules, ID)
}

//getSchedulables returnsn all schedules
func (s *service) getSchedulables() []*Schedulable {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var result = make([]*Schedulable, 0)
	for _, candidate := range s.schedules {
		result = append(result, candidate)
	}
	return result
}

//dueToRun returns runnable due to runner
func (s *service) dueToRun() []*Schedulable {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var result = make([]*Schedulable, 0)
	now := time.Now()
	for _, candidate := range s.schedules {
		if candidate.IsRunning() {
			continue
		}
		schedule := candidate.Schedule
		if schedule.IsDue(now) {
			result = append(result, candidate)
		}
	}
	return result
}

//run runner scheduler logic
func (s *service) run() {
	for {
		_ = s.load()
		dueToRun := s.dueToRun()
		if len(dueToRun) == 0 {
			time.Sleep(time.Second)
			continue
		}
		watGroup := &sync.WaitGroup{}
		watGroup.Add(len(dueToRun))
		now := time.Now()
		for i := range dueToRun {
			schedulalble := dueToRun[i]
			schedulalble.Schedule.Next(now)


			go func(schedulable *Schedulable) {
				watGroup.Done()
				defer schedulable.Done()
				runnable := schedulable.Clone()
				_ = runnable.Init()
				err := s.runner(runnable)
				if err != nil {
					schedulable.Schedule.ErrorCount++
					log.Printf("failed to run %v,%v", schedulable.ID, err)
					schedulable.ScheduleNexRun(time.Now().Add(time.Minute * time.Duration(schedulable.Schedule.ErrorCount%5)))

				}
				remaining := time.Second * time.Duration(schedulalble.Schedule.NextRun.Unix()-time.Now().Unix())
				log.Printf("[%v] next run at: %v, remaining %s\n", schedulalble.ID, schedulalble.Schedule.NextRun.Format(dateLayout), remaining)
			}(schedulalble)
		}
		watGroup.Wait() //wait only for re-scheduling completion, not runner completion
		time.Sleep(time.Second)
	}
}

func (s *service) load() error {
	isDueToLoad := time.Now().After(s.nextCheck)
	if !isDueToLoad {
		return nil
	}
	s.nextCheck = time.Now().Add(s.refreshDuration)
	resource := url.NewResource(s.Config.ScheduleURL)
	storageService, err := storage.NewServiceForURL(resource.URL, "")
	if err != nil {
		return err
	}
	var ids = make(map[string]bool)
	if err := s.loadFromURL(storageService, resource.URL, ids); err != nil {
		return err
	}
	s.removeUnknown(ids)
	return nil
}

func (s *service) loadFromURL(storageService storage.Service, URL string, ids map[string]bool) error {
	objects, err := storageService.List(URL)
	if err != nil {
		return err
	}
	for i := range objects {
		object := objects[i]
		if strings.Trim(URL, "/") == strings.Trim(object.URL(), "/") {
			continue
		}
		if object.IsFolder() {
			if err = s.loadFromURL(storageService, object.URL(), ids); err != nil {
				return err
			}
			continue
		}
		fileInfo := object.FileInfo()
		ext := path.Ext(fileInfo.Name())
		if ext != ".json" && ext != ".yaml" {
			continue
		}

		schedulable, err := NewSchedulableFromURL(object.URL())
		if err != nil {
			return err
		}
		schedulable.URL = object.URL()
		if err = schedulable.Init(); err == nil {
			err = schedulable.Validate()
		}
		if err != nil {
			log.Printf("err: %v  %v\n", object.URL(), err)
			continue
		}

		ids[schedulable.ID] = true
		if !s.hasChanged(schedulable.ID, fileInfo.ModTime()) {
			continue
		}
		s.add(schedulable, fileInfo.ModTime())
	}
	return nil
}

func (s *service) removeUnknown(known map[string]bool) {
	ids := s.listIDs()
	for _, id := range ids {
		if _, has := known[id]; !has {
			log.Printf("Removed job: %v\n", id)
			s.remove(id)
		}
	}
}

//New creates a scheduler
func New(config *shared.Config, runner Runner) (Service, error) {
	result := &service{
		runner:    runner,
		Config:    config,
		schedules: make(map[string]*Schedulable),
		modified:  make(map[string]time.Time),
		mutex:     &sync.Mutex{},
		nextCheck: time.Now().Add(-time.Second),
	}

	if config.ScheduleURL == "" {
		return result, nil
	}
	resource := url.NewResource(config.ScheduleURL)
	if !toolbox.FileExists(resource.ParsedURL.Path) {
		if err := toolbox.CreateDirIfNotExist(resource.ParsedURL.Path); err != nil {
			return nil, err
		}
	}
	scheduleURLRefreshMs := config.ScheduleURLRefreshMs
	if scheduleURLRefreshMs == 0 {
		scheduleURLRefreshMs = defaultSchedulerLoadFrequencyMs
	}
	result.refreshDuration = time.Millisecond * time.Duration(defaultSchedulerLoadFrequencyMs)
	var err error
	if err = result.load(); err == nil {
		go result.run()
	}
	return result, err
}
