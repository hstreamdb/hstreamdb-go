package util

import (
	"bytes"
	"github.com/hstreamdb/hstreamdb-go/client"
	hstreampb "github.com/hstreamdb/hstreamdb-go/gen-proto/hstream/server"
	"github.com/pkg/errors"
	"math/rand"
	"sort"
	"strings"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandomString(n int) string {
	var src = rand.NewSource(time.Now().UnixNano())
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

func RandomServer(client client.Client) (string, error) {
	infos, err := client.GetServerInfo()
	if err != nil {
		return "", errors.WithStack(err)
	}
	idx := rand.Intn(len(infos))
	return infos[idx], nil
}

// CompareRecordId compare two record id a and b, return
// positive number if a > b, negative number if a < b and 0 if a == b
func CompareRecordId(a, b *hstreampb.RecordId) int {
	if a.BatchId != b.BatchId {
		return int(a.GetBatchId() - b.GetBatchId())
	} else {
		return int(a.GetBatchIndex() - b.GetBatchIndex())
	}
}

// CompareReceivedRecord check if two record is equal
func CompareReceivedRecord(a, b *hstreampb.ReceivedRecord) bool {
	if CompareRecordId(a.GetRecordId(), b.GetRecordId()) != 0 {
		return false
	} else {
		return bytes.Equal(a.GetRecord(), b.GetRecord())
	}
}

type RecordIdList []*hstreampb.RecordId

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
	return CompareRecordId(r.RecordIdList[i], r.RecordIdList[j]) < 0
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
		if CompareRecordId(a.RecordIdList[i], b.RecordIdList[i]) != 0 {
			return false
		}
	}
	return true
}
