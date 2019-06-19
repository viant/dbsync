package history

//ShowRequest represent show request
type ShowRequest struct {
	ID string
}

//ShowResponse represent history response
type ShowResponse struct {
	Items []*Job
}

//StatusRequest represent past job status request
type StatusRequest struct {
	RunCount int
}

//StatusResponse represents past job status response
type StatusResponse struct {
	*Status
}

//NewStatusResponse return status response
func NewStatusResponse() *StatusResponse {
	return &StatusResponse{
		Status: NewStatus(),
	}
}
