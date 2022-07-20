package hstream

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
)

// ReceivedRecord is an interface for a record consumed from a stream.
// Currently, it has two instance: RawRecord and HRecord
type ReceivedRecord interface {
	GetRecordId() RecordId
	GetRecordType() RecordType
	GetPayload() interface{}
}

// RawRecord is a concrete receivedRecord with raw byte payload
type RawRecord struct {
	Header   RecordHeader
	RecordId RecordId
	Payload  []byte
}

func (r *RawRecord) GetRecordId() RecordId {
	return r.RecordId
}

func (r *RawRecord) GetRecordType() RecordType {
	return RAWRECORD
}

func (r *RawRecord) GetPayload() interface{} {
	return r.Payload
}

// HRecord is a concrete receivedRecord with json payload
type HRecord struct {
	Header   RecordHeader
	RecordId RecordId
	Payload  map[string]interface{}
}

func (h *HRecord) GetRecordId() RecordId {
	return h.RecordId
}

func (h *HRecord) GetRecordType() RecordType {
	return HRECORD
}

func (h *HRecord) GetPayload() interface{} {
	return h.Payload
}

type RecordType uint16

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

// RecordId is the unique identifier for a record
type RecordId struct {
	BatchId    uint64
	BatchIndex uint32
	ShardId    uint64
}

func (r RecordId) String() string {
	return fmt.Sprintf("%d-%d-%d", r.BatchId, r.BatchIndex, r.ShardId)
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

// RecordHeader is the header of a HStreamRecord
type RecordHeader struct {
	Key        string
	Flag       RecordType
	Attributes map[string]string
}

type HStreamRecord struct {
	Header  RecordHeader
	Key     string
	Payload []byte
}

func NewHStreamRawRecord(key string, payload []byte) (*HStreamRecord, error) {
	return &HStreamRecord{
		Header: RecordHeader{
			Key:        key,
			Flag:       RAWRECORD,
			Attributes: make(map[string]string),
		},
		Key:     key,
		Payload: payload,
	}, nil
}

func NewHStreamHRecord(key string, payload map[string]interface{}) (*HStreamRecord, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal payload")
	}
	return &HStreamRecord{
		Header: RecordHeader{
			Key:        key,
			Flag:       HRECORD,
			Attributes: make(map[string]string),
		},
		Key:     key,
		Payload: data,
	}, nil
}

func (r *HStreamRecord) GetKey() string {
	return r.Header.Key
}

func (r *HStreamRecord) GetType() RecordType {
	return r.Header.Flag
}
