package client

import (
	"github.com/hstreamdb/hstreamdb-go/gen-proto/hstream/server"
	"github.com/pkg/errors"
	"time"
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

type RecordHeader struct {
	tp          RecordType
	attributes  map[string]string
	publishTime time.Time
	key         string
}

type HStreamRecord interface {
	GetHeader() RecordHeader
	GetPayLoad() []byte
	ToHStreamRecord() *server.HStreamRecord
}

type RawRecord struct {
	header  RecordHeader
	payLoad []byte
}

func NewRawRecord(key string, payload []byte) *RawRecord {
	return &RawRecord{
		header: RecordHeader{
			tp:  RAWRECORD,
			key: key,
		},
		payLoad: payload,
	}
}

func (r *RawRecord) GetHeader() RecordHeader {
	return r.header
}

func (r *RawRecord) GetPayLoad() []byte {
	return r.payLoad
}

func (r *RawRecord) ToHStreamRecord() *server.HStreamRecord {
	return &server.HStreamRecord{
		Header: &server.HStreamRecordHeader{
			Flag:       server.HStreamRecordHeader_RAW,
			Attributes: r.header.attributes,
			Key:        r.header.key,
		},
		Payload: r.payLoad,
	}
}

func HStreamRecordToRawRecord(pbRecord *server.HStreamRecord) (*RawRecord, error) {
	if pbRecord.GetHeader().Flag != server.HStreamRecordHeader_RAW {
		return nil, errors.New("not a raw record")
	}
	header := pbRecord.GetHeader()
	return &RawRecord{
		header:  pbHeaderToRecordHeader(header),
		payLoad: pbRecord.GetPayload(),
	}, nil
}

type HRecord struct {
	header  RecordHeader
	payLoad []byte
}

func NewHRecord(key string, payload []byte) *HRecord {
	return &HRecord{
		header: RecordHeader{
			tp:  HRECORD,
			key: key,
		},
		payLoad: payload,
	}
}

func (h *HRecord) GetHeader() RecordHeader {
	return h.header
}

func (h *HRecord) GetPayLoad() []byte {
	return h.payLoad
}

func (h *HRecord) ToHStreamRecord() *server.HStreamRecord {
	return &server.HStreamRecord{
		Header: &server.HStreamRecordHeader{
			Flag:       server.HStreamRecordHeader_JSON,
			Attributes: h.header.attributes,
			Key:        h.header.key,
		},
		Payload: h.payLoad,
	}
}

func HStreamRecordToHRecord(pbRecord *server.HStreamRecord) (*HRecord, error) {
	if pbRecord.GetHeader().Flag != server.HStreamRecordHeader_JSON {
		return nil, errors.New("not a raw record")
	}
	header := pbRecord.GetHeader()
	return &HRecord{
		header:  pbHeaderToRecordHeader(header),
		payLoad: pbRecord.GetPayload(),
	}, nil
}

func pbHeaderToRecordHeader(header *server.HStreamRecordHeader) RecordHeader {
	var tp RecordType
	switch header.GetFlag() {
	case server.HStreamRecordHeader_RAW:
		tp = RAWRECORD
	case server.HStreamRecordHeader_JSON:
		tp = HRECORD
	}
	return RecordHeader{
		tp:          tp,
		attributes:  header.GetAttributes(),
		publishTime: header.GetPublishTime().AsTime(),
		key:         header.GetKey(),
	}
}
