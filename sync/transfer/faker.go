package transfer

import (
	"dbsync/sync/core"
	"dbsync/sync/shared"
)

type faker struct {
	Service
	transferred int
	err         error
}

func (f *faker) Post(ctx *shared.Context, request *Request, transferable *core.Transferable) error {
	if f.err != nil {
		return f.err
	}
	transferable.SetTransferred(f.transferred)
	return nil
}

//NewFaker returns new post faker
func NewFaker(service Service, transferred int, err error) Service {
	return &faker{
		Service:     service,
		transferred: transferred,
		err:         err,
	}
}
