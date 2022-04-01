package test_util

import (
	"sort"

	"github.com/hstreamdb/hstreamdb-go/hstream"
)

var ServerUrl = "localhost:6580,localhost:6581,localhost:6582"

type GatherRidsHandler struct {
	res     []hstream.RecordId
	maxSize int
	Err     error
	Done    chan struct{}
}

func MakeGatherRidsHandler(size int) *GatherRidsHandler {
	return &GatherRidsHandler{
		maxSize: size,
		res:     make([]hstream.RecordId, 0, size),
		Done:    make(chan struct{}),
	}
}

func (h *GatherRidsHandler) HandleRes(res hstream.FetchResult) {
	records, err := res.GetResult()
	if err != nil {
		h.Err = err
		return
	}

	for _, record := range records {
		h.res = append(h.res, record.GetRecordId())
		if len(h.res) >= h.maxSize {
			break
		}
	}
	h.Done <- struct{}{}
}

func (h *GatherRidsHandler) GetRes() []hstream.RecordId {
	return h.res
}

type RecordIdList []hstream.RecordId

func (r RecordIdList) Len() int {
	return len(r)
}

func (r RecordIdList) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type RecordIdComparator struct {
	RecordIdList
}

func (r RecordIdComparator) Less(i, j int) bool {
	return hstream.CompareRecordId(r.RecordIdList[i], r.RecordIdList[j]) < 0
}

func (r *RecordIdComparator) Sort() {
	sort.Sort(r)
}

// RecordIdComparatorCompare check if two RecordId list is equal
func RecordIdComparatorCompare(a, b RecordIdComparator) bool {
	sizeA := a.Len()
	sizeB := b.Len()
	if sizeA != sizeB {
		return false
	}

	for i := 0; i < sizeA; i++ {
		if hstream.CompareRecordId(a.RecordIdList[i], b.RecordIdList[i]) != 0 {
			return false
		}
	}
	return true
}
