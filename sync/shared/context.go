package shared

import "log"

type Context struct {
	Debug bool
}

func (c *Context) Log(v ... interface{}) {
	if c.Debug {
		log.Print(v...)
	}
}
