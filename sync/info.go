package sync

//Info represents a sync info
type Info struct {
	InSync        bool
	Method        string
	Inconsistency string
	SourceCount   int
	DestCount     int
	SyncFromID    int
	MinValue      int
	MaxValue      int
	SourceMax     int
}

//SetDestMaxID narrows sync info with matched dest ID in sync with source
func (i *Info) SetDestMaxID(destMaxID int, method string, info *Info) {
	i.Method = method
	i.SyncFromID = destMaxID
	i.MinValue = destMaxID + 1
	i.SourceCount -= info.SourceCount
	i.DestCount -= info.DestCount
}

func (i *Info) SetMethod(method string) {
	if i.Method == SyncMethodDeleteMerge || i.Method == SyncMethodDeleteInsert {
		return
	}
	if method == SyncMethodDeleteMerge || method == SyncMethodDeleteInsert {
		i.Method = method
		return
	}
	if i.Method == SyncMethodMerge {
		return
	}
	i.Method = method
}

func (i *Info) SetSourceMax(max int) {
	if i.SourceMax > max {
		return
	}
	i.SourceMax = max
}
