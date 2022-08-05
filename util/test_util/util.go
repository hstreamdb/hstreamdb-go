package test_util

import (
	"sort"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
)

var ServerUrl = "localhost:6580,localhost:6581,localhost:6582"

type RecordIdList []Record.RecordId

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
	return Record.CompareRecordId(r.RecordIdList[i], r.RecordIdList[j]) < 0
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
		if Record.CompareRecordId(a.RecordIdList[i], b.RecordIdList[i]) != 0 {
			return false
		}
	}
	return true
}
