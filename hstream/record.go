package hstream

import (
	"encoding/json"
	"fmt"

	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/pkg/errors"
)

type RecordType uint16

const (
	RAWRECORD RecordType = iota + 1
	HRECORD
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

type RecordId struct {
	BatchId    uint64
	BatchIndex uint32
	ShardId    uint64
}

func (r *RecordId) ToPbRecordId() *hstreampb.RecordId {
	return &hstreampb.RecordId{
		BatchId:    r.BatchId,
		BatchIndex: r.BatchIndex,
		ShardId:    r.ShardId,
	}
}

func (r *RecordId) String() string {
	return fmt.Sprintf("[BatchId: %d, BatchIndex: %d, ShardId: %d]", r.BatchId, r.BatchIndex, r.ShardId)
}

func FromPbRecordId(pb *hstreampb.RecordId) *RecordId {
	return &RecordId{
		BatchId:    pb.BatchId,
		BatchIndex: pb.BatchIndex,
		ShardId:    pb.ShardId,
	}
}

// CompareRecordId compare two record id a and b, return
// positive number if a > b, negative number if a < b and 0 if a == b
func CompareRecordId(a, b *RecordId) int {
	if a.BatchId != b.BatchId {
		return int(a.BatchIndex - b.BatchIndex)
	} else {
		return int(a.BatchId - b.BatchId)
	}
}

type ReceivedRecord interface {
	GetRecordId() *RecordId
}

//// CompareReceivedRecord check if two record is equal. Only check recordId now
//func CompareReceivedRecord(a, b hstream.ReceivedRecord) bool {
//	if CompareRecordId(a.GetRecordId(), b.GetRecordId()) != 0 {
//		return false
//	}
//	return true
//}

type RawRecord struct {
	RecordId *RecordId
	Payload  []byte
}

func (r *RawRecord) GetRecordId() *RecordId {
	return r.RecordId
}

func FromPbRawRecord(pb *hstreampb.ReceivedRecord) (*RawRecord, error) {
	return &RawRecord{
		RecordId: FromPbRecordId(pb.RecordId),
		Payload:  pb.Record,
	}, nil
}

type HRecord struct {
	RecordId *RecordId
	Payload  map[string]interface{}
}

func (h *HRecord) GetRecordId() *RecordId {
	return h.RecordId
}

func FromPbHRecord(pb *hstreampb.ReceivedRecord) (*HRecord, error) {
	var hRecord *HRecord
	if err := json.Unmarshal(pb.Record, &hRecord.Payload); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal hrecord")
	}
	hRecord.RecordId = FromPbRecordId(pb.RecordId)
	return hRecord, nil
}

type HStreamRecord interface {
	GetRecordKey() string
	GetRecordType() RecordType
	ToPbHStreamRecord() (*hstreampb.HStreamRecord, error)
}

type HStreamRawRecord struct {
	Key     string
	Payload []byte
}

func NewHStreamRawRecord(key string, payload []byte) *HStreamRawRecord {
	return &HStreamRawRecord{
		Key:     key,
		Payload: payload,
	}
}

func (h *HStreamRawRecord) GetRecordKey() string {
	return h.Key
}

func (h *HStreamRawRecord) GetRecordType() RecordType {
	return RAWRECORD
}

func (h *HStreamRawRecord) ToPbHStreamRecord() (*hstreampb.HStreamRecord, error) {
	return &hstreampb.HStreamRecord{
		Header: &hstreampb.HStreamRecordHeader{
			Flag:       hstreampb.HStreamRecordHeader_RAW,
			Attributes: make(map[string]string),
			Key:        h.Key,
		},
		Payload: h.Payload,
	}, nil
}

type HStreamHRecord struct {
	Key     string
	Payload map[string]interface{}
}

func NewHStreamHRecord(key string, payload map[string]interface{}) *HStreamHRecord {
	return &HStreamHRecord{
		Key:     key,
		Payload: payload,
	}
}

func (h *HStreamHRecord) GetRecordKey() string {
	return h.Key
}

func (h *HStreamHRecord) GetRecordType() RecordType {
	return HRECORD
}

func (h *HStreamHRecord) ToPbHStreamRecord() (*hstreampb.HStreamRecord, error) {
	records, err := json.Marshal(h.Payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal payload")
	}
	return &hstreampb.HStreamRecord{
		Header: &hstreampb.HStreamRecordHeader{
			Flag:       hstreampb.HStreamRecordHeader_RAW,
			Attributes: make(map[string]string),
			Key:        h.Key,
		},
		Payload: records,
	}, nil
}
