package data


//Index represents indexed Source and dest 
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