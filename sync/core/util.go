package core

import (
	"github.com/viant/toolbox"
	"math"
)

func round(value interface{}, numericPrecision int) float64 {
	f := toolbox.AsFloat(value)
	precisionPoint := float64(numericPrecision)
	unit := 1 / math.Pow(10, precisionPoint)
	return math.Round(f/unit) * unit
}
