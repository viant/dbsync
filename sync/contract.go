package sync

import "github.com/viant/toolbox/url"

type SyncRequest struct {
	Sync
	Transfer
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