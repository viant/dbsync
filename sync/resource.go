package sync

import (
	"dbsync/sync/pseudo"
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
	PseudoColumns     []*pseudo.Column
	columnExpression  map[string]*pseudo.Column
	PositionReference bool
	Criteria          map[string]interface{}
	ChunkSQL          string
}

//Validate checks if resource is valid
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
	r.columnExpression = make(map[string]*pseudo.Column)
	if len(r.PseudoColumns) == 0 {
		return
	}
	for _, column := range r.PseudoColumns {
		r.columnExpression[column.Name] = column
	}
}
