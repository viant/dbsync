package history

//Request represent history request
type ShowRequest struct {
	ID string
}

//Response represent history response
type ShowResponse struct {
	Items []*Job
}

type StatusRequest struct {
	RunCount int
}


type StatusResponse struct {
	*Status
}


//NewStatusResponse return status response
func NewStatusResponse() *StatusResponse {
	return &StatusResponse{
		Status:NewStatus(),
	}
}


