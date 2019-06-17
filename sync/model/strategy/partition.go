package strategy

import "dbsync/sync/shared"

const defaultPartitionThreads = 2



//PartitionStrategy represents partition info
type Partition struct {
	ProviderSQL string // Deprecated: use Source.PartitionSQL instead
	Columns     []string
	Threads     int
	SyncMode    string `description:"persistency sync mode: batched or individual"`
	BatchSize   int
}


//MaxThreads returns batch size for max elements
func (p *Partition) MaxThreads(max int) int {
	threads := p.Threads
	if max < threads {
		threads = max
	}
	return threads
}

//Init initializes partition
func (p *Partition) Init() error {
	if len(p.Columns) == 0 {
		p.Columns = make([]string, 0)
	}
	var threads = p.Threads
	if threads == 0 {
		p.Threads = defaultPartitionThreads
	}
	if p.SyncMode == "" {
		p.SyncMode = shared.SyncModeIndividual
	}
	return nil
}
