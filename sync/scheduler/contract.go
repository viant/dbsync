package scheduler


//ListRequest represents a list request
type ListRequest struct {}


//ListResponse represents a list response
type ListResponse struct {
	Items []*Schedulable
}