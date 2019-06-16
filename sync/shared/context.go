package shared

import "log"

//Context represent a context
type Context struct {
	Debug bool
	//ID request ID
	ID string
}

//Log logs
func (c *Context) Log(v ... interface{}) {
	if c.Debug {
		log.Print(v...)
	}
}


//NewContext returns new context
func NewContext(ID string, debug bool) *Context{
	return &Context{
		ID:ID,
		Debug:debug,
	}
}