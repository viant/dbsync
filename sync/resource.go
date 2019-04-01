package sync

import (
	"github.com/viant/dsc"
)

//Resource represents sync resource
type Resource struct {
	*dsc.Config
	Table         string
	Hint          string
	PseudoColumns []*PseudoColumn
	pseudoColumns map[string]*PseudoColumn
}

func (r *Resource) indexPseudoColumns() {
	r.pseudoColumns = make(map[string]*PseudoColumn)
	if len(r.PseudoColumns) == 0 {
		return
	}
	for _, column := range r.PseudoColumns {
		r.pseudoColumns[column.Name] = column
	}
}
