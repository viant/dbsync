package criteria

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)


type Criterion fmt.Stringer


type between struct {
	from int
	to   int
}

func (b between) String() string {
	return fmt.Sprintf("BETWEEN %v AND %v", b.from, b.to)
}

//NewBetween creates new betwee criterion
func NewBetween(from, to int) *between {
	return &between{from:from, to:to}
}

//NewLessOrEqual creates less of equal criterion
func NewLessOrEqual(value int) *lessOrEqual{
	return &lessOrEqual{value}
}

type lessOrEqual struct {
	value int
}

func (c lessOrEqual) String() string {
	return fmt.Sprintf(" <= %v", c.value)
}


type greaterThan struct {
	value int
}

func (c greaterThan) String() string {
	return fmt.Sprintf(" > %v", c.value)
}


//NewGraterThan creates grater than criterion
func NewGraterThan(value int) *greaterThan{
	return &greaterThan{value}
}



type greaterOrEqual struct {
	value int
}

func (c greaterOrEqual) String() string {
	return fmt.Sprintf(" => %v", c.value)
}


//NewGraterOrEqual creates grater or equal criterion
func NewGraterOrEqual(value int) *greaterOrEqual{
	return &greaterOrEqual{value}
}


func ToCriterion(k string, v interface{}) string {
	if greaterOrEqual, ok := v.(*greaterOrEqual); ok {
		return fmt.Sprintf("%v >= %v", k, greaterOrEqual.value)
	} else if greaterThan, ok := v.(*greaterThan); ok {
		return fmt.Sprintf("%v > %v", k, greaterThan.value)
	} else if lessOrEqual, ok := v.(*lessOrEqual); ok {
		return fmt.Sprintf("%v <= %v", k, lessOrEqual.value)
	} else if between, ok := v.(*between); ok {
		return fmt.Sprintf("%v BETWEEN %v AND %v", k, between.from, between.to)
	} else if toolbox.IsSlice(v) {
		aSlice := toolbox.AsSlice(v)
		var whereValues = make([]string, 0)
		for _, item := range aSlice {
			if intValue, err := toolbox.ToInt(item); err == nil {
				whereValues = append(whereValues, fmt.Sprintf(`%v`, intValue))
			} else {
				itemLiteral := toolbox.AsString(item)
				if strings.HasPrefix(itemLiteral, "(") && strings.HasSuffix(itemLiteral, ")") {
					whereValues = append(whereValues, fmt.Sprintf(`%v`, item))
				} else {
					whereValues = append(whereValues, fmt.Sprintf(`'%v'`, item))
				}
			}
		}
		return fmt.Sprintf("%v IN(%v)", k, strings.Join(whereValues, ","))
	} else if _, err := toolbox.ToInt(v); err == nil {
		return fmt.Sprintf("%v = %v", k, v)
	} else {
		literal := strings.TrimSpace(toolbox.AsString(v))
		lowerLiteral := strings.ToLower(literal)
		if strings.Contains(literal, ">") ||
			strings.Contains(literal, "<") ||
			strings.Contains(lowerLiteral, " null") {
			return fmt.Sprintf("%v %v", k, v)
		}
		return fmt.Sprintf("%v = '%v'", k, v)
	}
}
