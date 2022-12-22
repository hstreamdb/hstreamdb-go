package test_util

import (
	"fmt"
	"sort"
	"time"

	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
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

func GenerateBatchHRecord(keySize, recordSize int) map[string][]Record.HStreamRecord {
	res := make(map[string][]Record.HStreamRecord, keySize)
	total := 0
	for i := 0; i < keySize; i++ {
		key := fmt.Sprintf("key-%d", i)
		hRecords := GenerateHRecordWithKey(key, recordSize)
		res[key] = hRecords
		total += len(hRecords)
	}
	util.Logger().Info("generate HRecord", zap.Int("count", total))
	return res
}

func GenerateBatchRawRecord(keySize, recordSize int) map[string][]Record.HStreamRecord {
	res := make(map[string][]Record.HStreamRecord, keySize)
	for i := 0; i < keySize; i++ {
		key := fmt.Sprintf("key-%d", i)
		hRecords := GenerateRawRecordWithKey(key, recordSize)
		res[key] = hRecords
	}
	return res
}

func GenerateHRecordWithKey(key string, recordSize int) []Record.HStreamRecord {
	payloads := make([]Record.HStreamRecord, 0, recordSize)
	for i := 0; i < recordSize; i++ {
		payload := map[string]interface{}{
			"key":       key,
			"value":     []byte(fmt.Sprintf("test-value-%s-%d", key, 1)),
			"timestamp": time.Now().UnixNano(),
		}
		hRecord, _ := Record.NewHStreamHRecord(key, payload)
		payloads = append(payloads, hRecord)
	}
	return payloads
}

func GenerateRawRecordWithKey(key string, recordSize int) []Record.HStreamRecord {
	payloads := make([]Record.HStreamRecord, 0, recordSize)
	for i := 0; i < recordSize; i++ {
		rawRecord, _ := Record.NewHStreamRawRecord(key, []byte(fmt.Sprintf("value-%d", i)))
		payloads = append(payloads, rawRecord)
	}
	return payloads
}
