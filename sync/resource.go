package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"strings"
)

//Resource represents sync resource
type Resource struct {
	*dsc.Config
	Table             string
	From              string
	Hint              string
	PseudoColumns     []*PseudoColumn
	columnExpression  map[string]*PseudoColumn
	PositionReference bool
	Criteria          map[string]interface{}
}

func (r *Resource) Validate() error {
	if len(r.PseudoColumns) == 0 {
		return nil
	}
	for _, column := range r.PseudoColumns {
		if strings.Index(column.Expression, "t.") == -1 {
			return fmt.Errorf("invalid pseudo column expectedion expected table alias t., but had: %v", column.Expression)
		}
	}
	return nil
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
