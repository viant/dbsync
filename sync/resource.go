package sync

import (
	"github.com/viant/dsc"
)

//Resource represents sync resource
type Resource struct {
	*dsc.Config
	Table      string
	DateColumn *PseudoColumn
	HourColumn *PseudoColumn
}

