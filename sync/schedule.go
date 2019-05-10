package sync

import (
	"fmt"
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

//ScheduleRunnable defines ScheduleRunnable contract
type ScheduleRunnable interface {
	ID() string
	ScheduledRun() (*Schedule, func(service Service) error)
}

//Scheduler represents basic scheduler
type Scheduler struct {
	*Config
	service         Service
	refreshDuration time.Duration
	runnables       map[string]ScheduleRunnable
	modified        map[string]time.Time
	mutex           *sync.Mutex
	nextCheck       time.Time
}

//Add adds runnable
func (s *Scheduler) Add(runnable ScheduleRunnable, modTime time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	log.Printf("Added schedule: %v\n", runnable.ID())
	s.runnables[runnable.ID()] = runnable
	s.modified[runnable.ID()] = modTime
}

//List lists runnable IDs
func (s *Scheduler) List() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var result = make([]string, 0)
	for k := range s.runnables {
		result = append(result, k)
	}
	return result
}

//Get returns runnable by ID
func (s *Scheduler) Get(ID string) ScheduleRunnable {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.runnables[ID]
}

//HasChanged returns runnable by ID
func (s *Scheduler) HasChanged(ID string, modTime time.Time) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	timeValue, ok :=  s.modified[ID]
	if ! ok {
		return true
	}
	return ! timeValue.Equal(modTime)
}


//Remove remove runnable by ID
func (s *Scheduler) Remove(ID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.runnables, ID)
}

//Scheduled returnsn all runnables
func (s *Scheduler) Scheduled() []ScheduleRunnable {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var result = make([]ScheduleRunnable, 0)
	for _, candidate := range s.runnables {
		result = append(result, candidate)
	}
	return result
}

//DueToRun returns runnable due to run
func (s *Scheduler) DueToRun() []ScheduleRunnable {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var result = make([]ScheduleRunnable, 0)
	for _, candidate := range s.runnables {
		schedule, _ := candidate.ScheduledRun()
		if schedule.Disabled {
			continue
		}
		isDueToRun := time.Now().After(*schedule.NextRun)
		if isDueToRun {
			result = append(result, candidate)
		}
	}
	return result
}

//Run run scheduler logic
func (s *Scheduler) Run() {
	for {
		_ = s.load()
		scheduled := s.DueToRun()
		if len(scheduled) == 0 {
			time.Sleep(time.Second)
			continue
		}
		watGroup := &sync.WaitGroup{}
		watGroup.Add(len(scheduled))
		for _, toRun := range scheduled {
			schedule, run := toRun.ScheduledRun()
			go func(schedule *Schedule, run func(service Service) error) {
				duration, _ := schedule.Frequency.Duration()
				schedule.SetNextRun(time.Now().Add(duration))
				watGroup.Done()
				err := run(s.service)
				schedule.RunCount++
				if err != nil {
					schedule.ErrorCount++
					log.Printf("failed to run %v,%v", toRun.ID(), err)
					schedule.SetNextRun(time.Now().Add(time.Minute * time.Duration(schedule.ErrorCount%5)))

				}
			}(schedule, run)
		}
		watGroup.Wait() //wait only for re-scheduling completion, not run completion
		time.Sleep(time.Second)
	}
}

func (s *Scheduler) load() error {
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

func (s *Scheduler) loadFromURL(storageService storage.Service, URL string, ids map[string]bool) error {
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

		request, err := NewRequestFromURL(object.URL())
		if err != nil {
			return err
		}
		if request.Id == "" {
			request.Id = urlToId(object.URL())
		}
		if err = request.Init(); err == nil {
			err = request.Validate()
		}
		if err != nil {
			log.Printf("failed to load scheule: %v, %v", object.URL(), err)
			continue
		}
		schedule, _ := request.ScheduledRun()
		if schedule == nil {
			log.Print(fmt.Sprintf("schedule %v was empty", request.ID()))
			continue
		}
		schedule.SourceURL = object.URL()
		ids[request.ID()] = true

		if ! s.HasChanged(request.ID(), fileInfo.ModTime()) {
			continue
		}
		now := time.Now()
		schedule.NextRun = &now
		s.Add(request, fileInfo.ModTime())
	}
	return nil
}

func (s *Scheduler) removeUnknown(known map[string]bool) {
	ids := s.List()
	for _, id := range ids {
		if _, has := known[id]; !has {
			log.Printf("Removed job: %v\n", id)
			s.Remove(id)
		}
	}
}

//NewScheduler creates a new scheduler
func NewScheduler(service Service, config *Config) (*Scheduler, error) {
	result := &Scheduler{
		service:   service,
		Config:    config,
		runnables: make(map[string]ScheduleRunnable),
		modified:  make(map[string]time.Time),
		mutex:     &sync.Mutex{},

		nextCheck: time.Now().Add(-time.Second),
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
		go result.Run()
	}
	return result, err
}
