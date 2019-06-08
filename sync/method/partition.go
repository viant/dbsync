package method

const defaultPartitionThreads = 2
const TransientTableSuffix = "_tmp"


//PartitionStrategy represents partition info
type Partition struct {
	ProviderSQL string
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
		p.SyncMode = SyncModeIndividual
	}
	return nil
}
