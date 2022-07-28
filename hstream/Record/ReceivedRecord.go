package Record

import (
	"google.golang.org/protobuf/types/known/structpb"
)

// ReceivedRecord is an interface for a record consumed from a stream.
// Currently, it has two instance: ReceivedRawRecord and ReceivedHRecord
type ReceivedRecord interface {
	GetRecordId() RecordId
	GetRecordType() RecordType
	GetPayload() interface{}
}

// ReceivedRawRecord is a concrete receivedRecord with raw byte payload
type ReceivedRawRecord struct {
	Header   RecordHeader
	RecordId RecordId
	Payload  []byte
}

func (r *ReceivedRawRecord) GetRecordId() RecordId {
	return r.RecordId
}

func (r *ReceivedRawRecord) GetRecordType() RecordType {
	return RAWRECORD
}

func (r *ReceivedRawRecord) GetPayload() interface{} {
	return r.Payload
}

// ReceivedHRecord is a concrete receivedRecord with json payload
type ReceivedHRecord struct {
	Header   RecordHeader
	RecordId RecordId
	Payload  *structpb.Struct
}

func (h *ReceivedHRecord) GetRecordId() RecordId {
	return h.RecordId
}

func (h *ReceivedHRecord) GetRecordType() RecordType {
	return HRECORD
}

func (h *ReceivedHRecord) GetPayload() interface{} {
	return h.Payload
}
