package sync

import (
	"dbsync/sync/scheduler"
	"fmt"
	"github.com/viant/toolbox"
	"net/http"
)

const baseURI = "/v1/api"

//Router represents a router
type Router struct {
	*http.ServeMux
	service Service
}

func (r Router) route() {
	r.ServeMux.Handle(baseURI+"/", r.api())
	r.ServeMux.Handle("/", r.static())
	r.ServeMux.Handle("/status", r.status())
}

func (r Router) api() http.Handler {
	router := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        fmt.Sprintf("%v/sync", baseURI),
			Handler:    r.service.Sync,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/jobs", baseURI),
			Handler:    r.service.Jobs().List,
			Parameters: []string{""},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/job/{ids}", baseURI),
			Handler:    r.service.Jobs().List,
			Parameters: []string{"ids"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/schedules", baseURI),
			Handler: func() *scheduler.ListResponse {
				return r.service.Scheduler().List(&scheduler.ListRequest{})
			},
			Parameters: []string{},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/history/{id}", baseURI),
			Parameters: []string{"id"},
			Handler:    r.service.History().Show,
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/status", baseURI),
			Parameters: []string{"id"},
			Handler:    r.service.History().Status,
		},
	)



	return http.HandlerFunc(func(writer http.ResponseWriter, reader *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				var err = fmt.Errorf("%v", r)
				http.Error(writer, err.Error(), 500)
			}
		}()
		if err := router.Route(writer, reader); err != nil {
			http.Error(writer, err.Error(), 500)
		}
	})
}

func (r Router) static() http.Handler {
	return http.FileServer(http.Dir("static"))
}

func (r Router) status() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	})
}

//NewRouter creates a new router
func NewRouter(service Service) http.Handler {
	var result = &Router{
		ServeMux: http.NewServeMux(),
		service:  service,
	}
	result.route()
	return result
}
