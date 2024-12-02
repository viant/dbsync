package contract

import (
	"dbsync/sync/contract/strategy"
	"dbsync/sync/shared"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/toolbox/url"
)

const defaultPartitionBatchSize = 16

// Sync represents sync instruction
type Sync struct {
	strategy.Strategy `yaml:",inline" json:",inline"`
	Transfer          Transfer
	PreserveCase      bool
	Dest              *Resource
	Source            *Resource
	Table             string
	Criteria          map[string]interface{}
	Schedule          *Schedule
	Lock              *bool
	Async             bool
	Debug             bool
	DMLTimeout        int
	UseCreateLikeDDL  bool
}

// UseLock returns lock flag
func (r *Sync) UseLock() bool {
	if r.Lock != nil {
		return *r.Lock
	}
	return true
}

func (r *Sync) Clone() *Sync {
	return &Sync{
		Strategy:         *r.Strategy.Clone(),
		Transfer:         r.Transfer,
		Dest:             r.Dest,
		Lock:             r.Lock,
		Source:           r.Source,
		Table:            r.Table,
		Criteria:         r.Criteria,
		Schedule:         r.Schedule,
		Async:            r.Async,
		UseCreateLikeDDL: r.UseCreateLikeDDL,
		Debug:            r.Debug,
	}
}

// Init initialized Request
func (r *Sync) Init() error {
	if r.Dest == nil || r.Source == nil {
		return nil
	}

	err := r.Dest.Init()
	if err == nil {
		err = r.Source.Init()
	}
	if err != nil {
		return err
	}

	if r.Partition.ProviderSQL != "" {
		r.Source.PartitionSQL = r.Partition.ProviderSQL
	}

	if r.MergeStyle == "" {
		if r.Dest != nil {
			switch r.Dest.DriverName {
			case "mysql":
				r.MergeStyle = shared.DMLInsertOnDuplicateUpddate
			case "sqlite3":
				r.MergeStyle = shared.DMLInsertOrReplace
			case "oci8", "ora":
				r.MergeStyle = shared.DMLMergeInto
			default:
				r.MergeStyle = shared.DMLMerge
			}
		}
	}

	if err := r.Transfer.Init(); err != nil {
		return err
	}
	if err := r.Strategy.Init(); err != nil {
		return err
	}
	if len(r.Criteria) > 0 {
		if len(r.Dest.Criteria) == 0 {
			r.Dest.Criteria = r.Criteria
		}
		if len(r.Source.Criteria) == 0 {
			r.Source.Criteria = r.Criteria
		}
	}
	if r.Diff.BatchSize > 0 && r.Partition.BatchSize == 0 {
		r.Partition.BatchSize = r.Diff.BatchSize
	}
	if r.Partition.BatchSize == 0 {
		r.Partition.BatchSize = defaultPartitionBatchSize
	}

	if r.Table != "" {
		if r.Dest.Table == "" {
			r.Dest.Table = r.Table
		}
		if r.Source.Table == "" {
			r.Source.Table = r.Table
		}
	}
	if r.Schedule != nil {
		if r.Schedule.At != nil {
			return r.Schedule.At.Init()
		}
	}
	if err = r.Partition.Init(); err == nil {
		err = r.Diff.Init()
	}
	return err
}

// Validate checks if Request is valid
func (r *Sync) Validate() error {
	if r.Source == nil {
		return fmt.Errorf("source was empty")
	}
	if r.Dest == nil {
		return fmt.Errorf("dest was empty")
	}
	if r.Dest.Table == "" {
		return fmt.Errorf("dest table was empty")
	}
	if r.Source.PartitionSQL != "" {
		if len(r.Partition.Columns) == 0 {
			return fmt.Errorf("partition.columns were empty")
		}
	}

	if r.Transfer.EndpointIP == "" {
		return fmt.Errorf("transfer.endpointIP was empty")
	}

	if r.Diff.CountOnly && len(r.Diff.Columns) > 0 {
		return fmt.Errorf("countOnly can not be set with custom columns")
	}
	if r.Chunk.Size > 0 && len(r.IDColumns) != 1 {
		return fmt.Errorf("data chunking is only supported with single unique key, chunk size: %v, unique key count: %v", r.Chunk.Size, len(r.IDColumns))
	}

	if r.Schedule != nil {
		if err := r.Schedule.Validate(); err != nil {
			return err
		}
	}
	if err := r.Source.Validate(); err != nil {
		return errors.Wrap(err, "invalid source")
	}
	if err := r.Dest.Validate(); err != nil {
		return errors.Wrap(err, "invalid dest")
	}
	return nil
}

// NewSyncFromURL returns new sync from URL
func NewSyncFromURL(URL string) (*Sync, error) {
	result := &Sync{}
	resource := url.NewResource(URL)
	return result, resource.Decode(result)
}
