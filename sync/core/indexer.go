package core

//Indexer represents an indexer
type Indexer struct {
	dateLayout string
	keys       []string
}

//Index indexes records
func (i *Indexer) Index(source, dest Records) *Index {
	result := NewIndex()
	i.index(source, result.Source)
	i.index(dest, result.Dest)
	return result
}

func (i *Indexer) index(records Records, target map[string]Record) {
	for j := range records {
		indexValue := records[j].NormIndex(i.dateLayout, i.keys)
		target[indexValue] = records[j]
	}
}

//NewIndexer creates a new indexer
func NewIndexer(keys []string, dateLayout string) *Indexer {
	return &Indexer{
		dateLayout: dateLayout,
		keys:       keys,
	}
}
