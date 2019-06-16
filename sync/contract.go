package sync

import (
	"dbsync/sync/model"
	"dbsync/sync/shared"
	"github.com/viant/toolbox/url"
)

//Request represnet sync1 request
type Request struct {
	Id string

	*model.Sync
	Schedule *model.Schedule
}

//Response return response
type Response struct {
	JobID       string `json:",ommitempty"`
	Status      string
	Transferred int
	SourceCount int
	DestCount   int
	Error       string `json:",ommitempty"`
}

//NewRequestFromURL create a new request from URL
func NewRequestFromURL(URL string) (*Request, error) {
	resource := url.NewResource(URL)
	result := &Request{}
	return result, resource.Decode(result)
}

//ID returns sync1 request ID
func (r *Request) ID() string {
	if r.Id != "" {
		return r.Id
	}
	_ = r.Dest.Config.Init()
	if r.Dest.Config.Has("dbname") {
		return r.Dest.Config.Get("dbname") + ":" + r.Dest.Table
	}
	return r.Dest.Table
}

//SetError sets error message and returns true if err was not nil
func (r *Response) SetError(err error) bool {
	if err == nil {
		return false
	}
	r.Status = shared.StatusError
	r.Error = err.Error()
	return true
}
