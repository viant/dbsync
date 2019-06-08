package data


//Index represents indexed source and dest 
type Index struct {
	Source map[string]Record
	Dest   map[string]Record
}

func NewIndex() *Index{
	return &Index{
		Source:make(map[string]Record),
		Dest:make(map[string]Record),
	}
}