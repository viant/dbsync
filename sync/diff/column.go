package diff

import (
	"fmt"
	"github.com/viant/toolbox"
)

//Column represents column expression for computing difference
type Column struct {
	Name             string
	Func             string
	Default          interface{}
	NumericPrecision int
	DateFormat       string
	DateLayout       string
	Alias            string
}

//Expr returns expression
func (c *Column) Expr() string {
	column := c.Name
	if c.Default != nil {
		if toolbox.IsString(c.Default) {
			column = fmt.Sprintf("COALESCE(%v, '%v')", c.Name, c.Default)
		} else {
			column = fmt.Sprintf("COALESCE(%v, %v)", c.Name, c.Default)
		}
	}
	switch c.Func {
	case "COUNT":
		if column == "1" {
			return fmt.Sprintf("COUNT(%v) AS %v", column, c.Alias)
		}
		return fmt.Sprintf("COUNT(DISTINCT %v) AS %v", column, c.Alias)
	default:
		return fmt.Sprintf("%v(%v) AS %v", c.Func, column, c.Alias)
	}
}
