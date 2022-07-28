package Record

import "fmt"

type RecordType uint8

const (
	// RAWRECORD is the type for byte payload
	RAWRECORD RecordType = iota + 1
	// HRECORD is the type for json payload
	HRECORD
	// UNKNOWN is the type for unknown payload
	UNKNOWN
)

func (r RecordType) String() string {
	switch r {
	case RAWRECORD:
		return "RAWRECORD"
	case HRECORD:
		return "HRECORD"
	default:
		return "UNKNOWN"
	}
}

// RecordHeader is the header of a HStreamRecord
type RecordHeader struct {
	Key        string
	Flag       RecordType
	Attributes map[string]string
}

// RecordId is the unique identifier for a record
type RecordId struct {
	BatchId    uint64
	BatchIndex uint32
	ShardId    uint64
}

func (r RecordId) String() string {
	return fmt.Sprintf("%d-%d-%d", r.ShardId, r.BatchId, r.BatchIndex)
}

// CompareRecordId compare two record id a and b, return
// positive number if a > b, negative number if a < b and 0 if a == b
func CompareRecordId(a, b RecordId) int {
	if a.BatchId != b.BatchId {
		return int(a.BatchIndex - b.BatchIndex)
	} else {
		return int(a.BatchId - b.BatchId)
	}
}
