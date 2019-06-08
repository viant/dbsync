package dao

import (
	"dbsync/sync/data"
	"dbsync/sync/method"
	"github.com/viant/dsc"
)



//GetPartitions returns partition
func GetPartitions(manager dsc.Manager, strategy *method.Strategy) (*data.Partitions, error) {
	partitionStrategy := strategy.Partition
	
	var partitionsRecords = make([]data.Record, 0)
	if partitionStrategy.ProviderSQL != "" {
		if err := manager.ReadAll(&partitionsRecords, partitionStrategy.ProviderSQL, nil, nil); err != nil {
			return nil, err
		}
	}
	if len(partitionsRecords) == 0 {
		partitionsRecords = append(partitionsRecords, data.Record{})
	}

	var partitions = make([]*data.Partition, 0)
	for _, values := range partitionsRecords {
		partition := data.NewPartition(strategy, values)
		partition.Init()
		partitions = append(partitions, data.NewPartition(strategy, values))
	}

	result := data.NewPartitions(partitions, strategy)
	result.Init()
	return result, nil
}
