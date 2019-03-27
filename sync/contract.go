package sync

import "github.com/viant/toolbox/url"

type SyncRequest struct {
	Sync `yaml:",inline" json:",inline"`
	Transfer `yaml:",inline" json:",inline"`
	Dest *Resource
	Source *Resource
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
	return nil
}

//Validate checks if request is valid
func (r *SyncRequest) Validate() error {
	return nil
}



//SyncRequestFromURL creates a new resource from URL
func NewSyncRequestFromURL(URL string) (*SyncRequest, error) {
	request := &SyncRequest{}
	resource := url.NewResource(URL)
	return request, resource.Decode(request)
}