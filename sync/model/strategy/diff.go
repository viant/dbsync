package strategy

import (
	"dbsync/sync/model/strategy/diff"
	"github.com/viant/toolbox"
)

const defaultDiffBatchSize = 512

//DiffStrategy represents difference signature computation strategy
type Diff struct {
	Append           bool `description:"if specified only records where dest.ID > source.ID will be transfer "`
	Columns          []*diff.Column
	CountOnly        bool
	Depth            int `description:"controls detection of data that is similar"`
	BatchSize        int
	NumericPrecision int
	DateFormat       string
	DateLayout       string
}

//Init initializes diff
func (d *Diff) Init() error {
	if d.BatchSize == 0 {
		d.BatchSize = defaultDiffBatchSize
	}
	if d.NumericPrecision == 0 {
		d.NumericPrecision = 5
	}
	if d.DateFormat == "" && d.DateLayout == "" {
		d.DateLayout = toolbox.DateFormatToLayout("yyyy-MM-dd hh:mm:ss")
	} else if d.DateFormat != "" {
		d.DateLayout = toolbox.DateFormatToLayout(d.DateFormat)
	}
	return nil
}

//NewDiff creates new diff strategy
func NewDiff(columns ...*diff.Column) *Diff {
	return &Diff{
		Columns: columns,
	}
}
