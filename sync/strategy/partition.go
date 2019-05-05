package strategy

const defaultPartitionThreads = 2


//PartitionStrategy represents partition info
type Partition struct {
	ProviderSQL string
	Columns     []string
	Threads     int
}



//MaxThreads returns batch size for max elements
func (p *Partition) MaxThreads(max int) int {
	threads := p.Threads
	if max < threads {
		threads = max
	}
	return threads
}


func (p *Partition) Init() error {
	if len(p.Columns) == 0 {
		p.Columns = make([]string, 0)
	}
	var threads = p.Threads
	if threads == 0 {
		p.Threads = defaultPartitionThreads
	}
	return nil
}

