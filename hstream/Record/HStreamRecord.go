package Record

import (
	"google.golang.org/protobuf/types/known/structpb"
)

// HStreamRecord is used to represent a record written to hstream cluster
type HStreamRecord interface {
	GetKey() string
	hstreamRecordMarker()
}

type HRecord struct {
	Key     string
	Payload *structpb.Struct
}

func (h *HRecord) GetKey() string {
	return h.Key
}

func (h *HRecord) hstreamRecordMarker() {}

func NewHStreamHRecord(key string, payload map[string]interface{}) (HStreamRecord, error) {
	p, err := structpb.NewStruct(payload)
	if err != nil {
		return nil, err
	}
	return &HRecord{
		Key:     key,
		Payload: p,
	}, nil
}

type RawRecord struct {
	Key     string
	Payload []byte
}

func (r *RawRecord) GetKey() string {
	return r.Key
}

func (r *RawRecord) hstreamRecordMarker() {}

func NewHStreamRawRecord(key string, payload []byte) (HStreamRecord, error) {
	return &RawRecord{
		Key:     key,
		Payload: payload,
	}, nil
}
