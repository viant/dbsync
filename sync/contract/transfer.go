package contract

//Transfer represents transferData config
type Transfer struct {
	BatchSize     int
	WriterThreads int
	EndpointIP    string
	MaxRetries    int
	TempDatabase  string
	Suffix        string
}

//Init initializes transfer
func (t *Transfer) Init() error {
	if t.MaxRetries == 0 {
		t.MaxRetries = 2
	}
	return nil
}

