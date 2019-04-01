package transfer

import (
	"fmt"
	"github.com/viant/toolbox"
	"net/http"
)

const baseURI = "/v1/api"

type Router struct {
	*http.ServeMux
	service *Service
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
			URI:        fmt.Sprintf("%v/transfer", baseURI),
			Handler:    r.service.Transfer,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/tasks", baseURI),
			Handler:    r.service.Tasks,
			Parameters: []string{},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/task/{id}", baseURI),
			Handler:    r.service.Task,
			Parameters: []string{"id", "@httpResponseWriter"},
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
		writer.Write([]byte("ok"))
	})
}

func NewRouter(dummyService *Service) http.Handler {
	var result = &Router{
		ServeMux: http.NewServeMux(),
		service:  dummyService,
	}
	result.route()
	return result
}
