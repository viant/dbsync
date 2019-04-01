package sync

import (
	"fmt"
	"github.com/viant/toolbox/url"
)

type SyncRequest struct {
	Sync `yaml:",inline" json:",inline"`
	Transfer `yaml:",inline" json:",inline"`
	Dest *Resource
	Source *Resource
	Table string
}



type SyncResponse struct {
	Status string
	Error string
}


func (r * SyncResponse) SetError(err error) bool {
	if err == nil {
		return  false
	}
	r.Status = "error"
	r.Error = err.Error()
	return true
}

//Init initialized request
func (r *SyncRequest) Init() error {
	if r.MergeStyle == "" {
		if r.Dest != nil {
			switch r.Dest.DriverName {
			case "mysql":
				r.MergeStyle = DMLInsertUpddate
			case "sqlite3":
				r.MergeStyle = DMLInsertReplace
			default:
				r.MergeStyle = DMLMerge
			}
		}
	}
	if len(r.Partition.Columns) == 0 {
		r.Partition.Columns = make([]string, 0)
	}

	if r.Dest == nil || r.Source == nil {
		return nil
	}
	if r.Table != "" && r.Dest.Table == "" {
		r.Dest.Table = r.Table
	}
	if r.Table != "" && r.Source.Table == "" {
		r.Source.Table = r.Table
	}
	if r.Transfer.MaxRetries == 0 {
		r.Transfer.MaxRetries = 2
	}
	return nil
}

//Validate checks if request is valid
func (r *SyncRequest) Validate() error {
	if r.Source == nil {
		return fmt.Errorf("source was empty")
	}
	if r.Dest == nil {
		return fmt.Errorf("dest was empty")
	}
	if r.Dest.Table == "" {
		return fmt.Errorf("dest table was empty")
	}
	return nil
}



//SyncRequestFromURL creates a new resource from URL
func NewSyncRequestFromURL(URL string) (*SyncRequest, error) {
	request := &SyncRequest{}
	resource := url.NewResource(URL)
	return request, resource.Decode(request)
}