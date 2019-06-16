package jobs

import (
	"dbsync/sync/core"
)

//ListRequest represents a list request
type ListRequest struct {
	IDs []string //if empty return all job
}

//ListResponse represents a list response
type ListResponse struct {
	Jobs []*core.Job
}

