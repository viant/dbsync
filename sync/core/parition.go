package core

import (
	"dbsync/sync/criteria"
	"dbsync/sync/contract/strategy"
	"dbsync/sync/shared"
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

const partitionKeyTimeLayout = "20060102150405"

//Partition represents a partition
type Partition struct {
	*strategy.Strategy
	IDColumn string
	Transferable
	*Chunks
	err error
}

//BatchTransferable returns batched transferable
func (p *Partition) BatchTransferable() *Transferable {
	result := &Transferable{

		Suffix:p.Suffix,
		Status: &Status{
			Method:shared.SyncMethodInsert,
			Source:&Signature{},
			Dest:&Signature{},
		},
	}

	chunks := p.chunks
	for i:=0;i<len(chunks);i++ {
		transferable := chunks[i].Transferable
		if transferable.ShouldDelete() {
			continue
		}
		if transferable.Status == nil {
			continue
		}
		if transferable.Method != shared.SyncMethodInsert {
			result.Method = transferable.Method
		}
		result.Source.CountValue = result.Source.Count() + transferable.Source.Count()
	}
	return result
}



//SetError set errors
func (p *Partition) SetError(err error) {
	if err == nil {
		return
	}
	p.err = err
	p.CloseOffer()
}

//AddChunk add chunks
func (p *Partition) AddChunk(chunk *Chunk) {
	for k, v := range p.Filter {
		if _, has := chunk.Filter[k]; has {
			continue
		}
		chunk.Filter[k] = v
	}
	chunk.Index = p.ChunkSize()
	chunk.Suffix = fmt.Sprintf("%v_chunk_%05d", p.Suffix, chunk.Index)
	chunk.Filter[p.IDColumn] = criteria.NewBetween(chunk.Min(), chunk.Max())
	p.Chunks.Offer(chunk)
}

func (p *Partition) buildSuffix() string {
	suffix := shared.TransientTableSuffix
	columns := p.Partition.Columns
	var suffixValue = make([]string, 0)
	if len(columns) > 0 {
		for _, column := range columns {
			value, ok := p.Filter.Value(column)
			if ! ok {
				continue
			}
			if toolbox.IsTime(value) {
				timeValue, _ := toolbox.ToTime(value, "")
				value = timeValue.Format(partitionKeyTimeLayout)
			}
			if toolbox.IsSlice(value) {
				aSlice := toolbox.AsSlice(value)
				if len(aSlice) == 0 {
					continue
				}
				value = aSlice[0]
			}
			suffixValue = append(suffixValue, toolbox.AsString(value))
		}
		suffix += strings.Join(suffixValue, "_")

	}
	suffix = strings.Replace(suffix, "-", "", strings.Count(suffix, "-"))
	suffix = strings.Replace(suffix, "+", "", strings.Count(suffix, "+"))
	return suffix
}

//Init initializes partition
func (p *Partition) Init() {
	if p.Strategy == nil {
		p.Strategy = &strategy.Strategy{}
	}
	if len(p.IDColumns) == 1 {
		p.IDColumn = p.IDColumns[0]
	}
	p.Suffix = p.buildSuffix()
}

//InitWithMethod initializes with supplied method and suffix
func (p *Partition) InitWithMethod(method, suffix string) {
	p.IDColumn = p.IDColumns[0]
	p.Suffix = p.buildSuffix() + suffix
	p.Status = &Status{
		Method: method,
		Source: &Signature{},
		Dest:   &Signature{},
	}
}

//NewPartition returns new partition
func NewPartition(strategy *strategy.Strategy, record Record) *Partition {
	return &Partition{
		Strategy:     strategy,
		Transferable: Transferable{Filter: record},
		Chunks:       NewChunks(&strategy.Chunk),
	}
}
