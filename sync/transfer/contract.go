package transfer

import (
	"fmt"
	"github.com/viant/dsc"
)

//Source source
type Source struct {
	*dsc.Config
	Query string
}

//Dest dest
type Dest struct {
	*dsc.Config
	Table string
}


//Request represents transfer service request
type Request struct {
	Source      *Source
	Dest        *Dest
	WriterCount int
	Async       bool
	BatchSize   int
	Mode        string
	OmitEmpty   bool
}

//Response represents transfer service response
type Response struct {
	TaskID     int
	Status     string
	Error      string
	WriteCount int
}


//Error represents transfer error
type Error struct {
	*Response
}

//Error error interface
func (r *Error) Error() string {
	return fmt.Sprintf("%v", r.Response.Error)
}

//NewError creates a new transferData error
func NewError(response *Response) *Error {
	return &Error{
		Response: response,
	}
}


//IsTransferError returns true if error is TransferError type
func IsTransferError(err error) bool {
	_, ok := err.(*Error)
	return ok
}
