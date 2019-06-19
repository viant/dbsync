package contract

import (
	"dbsync/sync/contract/strategy/pseudo"
	"fmt"
	"github.com/viant/dsc"
	"strings"
)

const (
	//ResourceKindSource represents source resource
	ResourceKindSource = "source"
	//ResourceKindDest represents dest resource
	ResourceKindDest = "dest"
)

//ResourceKind represents source or dest
type ResourceKind string

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
	PartitionSQL      string
}

//Validate checks if resource is valid
func (r *Resource) Validate() error {
	if len(r.PseudoColumns) == 0 {
		return nil
	}
	for _, column := range r.PseudoColumns {
		if strings.Contains(strings.ToLower(column.Expression), "null") {
			continue
		}
		if strings.Index(column.Expression, "t.") == -1 {
			return fmt.Errorf("invalid pseudo column expectedion expected table formatColumn t., but had: %v", column.Expression)
		}
	}
	return nil
}

//GetPseudoColumn returns pseudo columns
func (r *Resource) GetPseudoColumn(column string) *pseudo.Column {
	return r.columnExpression[column]
}

//Init initializes resource
func (r *Resource) Init() error {
	r.indexPseudoColumns()
	if r.Config == nil {
		return nil
	}
	return r.Config.Init()
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

//ColumnExpr returns column expresion
func (r *Resource) ColumnExpr(column string) string {
	if pseudoColumn, ok := r.columnExpression[column]; ok {
		return pseudoColumn.Expression
	}
	return column
}
