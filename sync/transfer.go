package sync

//Transfer represents transfer config
type Transfer struct {
	BatchSize     int
	WriterThreads int
	URL           string
	MaxRowCount   int
	MultiTransfer int
}
