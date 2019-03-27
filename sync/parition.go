package sync

type Partition struct {
	ProviderSQL string
	IsDateBased bool
	Columns     []string
}
