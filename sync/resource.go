package sync

import (
	"github.com/viant/dsc"
)

//Resource represents sync resource
type Resource struct {
	*dsc.Config
	Table            string
	From             string
	Hint             string
	PseudoColumns    []*PseudoColumn
	columnExpression map[string]*PseudoColumn
	PositionReference bool
}

func (r *Resource) indexPseudoColumns() {
	r.columnExpression = make(map[string]*PseudoColumn)
	if len(r.PseudoColumns) == 0 {
		return
	}
	for _, column := range r.PseudoColumns {
		r.columnExpression[column.Name] = column
	}
}
