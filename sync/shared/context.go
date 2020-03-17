package shared

import (
	"fmt"
	"log"
)

//Context represent a context
type Context struct {
	UseLock bool
	Debug   bool
	//ID request ID
	ID string
}

//Log logs
func (c *Context) Log(v ...interface{}) {
	if c.Debug {
		if len(v) > 0 {
			v[0] = fmt.Sprintf("[%v] %v", c.ID, v[0])
		}
		log.Print(v...)
	}
}

//NewContext returns new context
func NewContext(ID string, debug bool, useLock bool) *Context {
	return &Context{
		ID:      ID,
		Debug:   debug,
		UseLock: useLock,
	}
}
