package hstream

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func RecordTypeFromPb(pb hstreampb.HStreamRecordHeader_Flag) (RecordType, error) {
	switch pb {
	case hstreampb.HStreamRecordHeader_RAW:
		return RAWRECORD, nil
	case hstreampb.HStreamRecordHeader_JSON:
		return HRECORD, nil
	default:
		return UNKNOWN, errors.Errorf("unknown record type: %s", pb)
	}
}

func RecordTypeToPb(r RecordType) (flag hstreampb.HStreamRecordHeader_Flag) {
	switch r {
	case RAWRECORD:
		flag = hstreampb.HStreamRecordHeader_RAW
	case HRECORD:
		flag = hstreampb.HStreamRecordHeader_JSON
	}
	return
}

func RecordIdToPb(r RecordId) *hstreampb.RecordId {
	return &hstreampb.RecordId{
		BatchId:    r.BatchId,
		BatchIndex: r.BatchIndex,
		ShardId:    r.ShardId,
	}
}

func RecordIdFromPb(pb *hstreampb.RecordId) RecordId {
	return RecordId{
		BatchId:    pb.BatchId,
		BatchIndex: pb.BatchIndex,
		ShardId:    pb.ShardId,
	}
}

func RecordHeaderToPb(r *RecordHeader) *hstreampb.HStreamRecordHeader {
	pb := &hstreampb.HStreamRecordHeader{
		Key:  r.Key,
		Flag: RecordTypeToPb(r.Flag),
	}
	if len(r.Attributes) > 0 {
		pb.Attributes = make(map[string]string)
		for k, v := range r.Attributes {
			pb.Attributes[k] = v
		}
	}
	return pb
}

func RecordHeaderFromPb(pb *hstreampb.HStreamRecordHeader) (RecordHeader, error) {
	flag, err := RecordTypeFromPb(pb.GetFlag())
	if err != nil {
		util.Logger().Error("failed to parse record type: %s", zap.Error(err))
		return RecordHeader{}, err
	}
	return RecordHeader{
		Key:        pb.GetKey(),
		Flag:       flag,
		Attributes: pb.GetAttributes(),
	}, nil
}

func HStreamRecordToPb(r *HStreamRecord) *hstreampb.HStreamRecord {
	header := RecordHeaderToPb(&r.Header)
	return &hstreampb.HStreamRecord{
		Header:  header,
		Payload: r.Payload,
	}
}

func ReceivedRecordFromPb(record *hstreampb.ReceivedRecord) (ReceivedRecord, error) {
	hstreamRecord := &hstreampb.HStreamRecord{}
	if err := proto.Unmarshal(record.GetRecord(), hstreamRecord); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal hstream record")
	}

	switch hstreamRecord.GetHeader().GetFlag() {
	case hstreampb.HStreamRecordHeader_RAW:
		return fromRPCRawRecord(record.GetRecordId(), hstreamRecord)
	case hstreampb.HStreamRecordHeader_JSON:
		return fromRPCHRecord(record.GetRecordId(), hstreamRecord)
	default:
		return nil, errors.Errorf("unknown record type: %s", hstreamRecord.GetHeader().GetFlag())
	}
}

func fromRPCRawRecord(rid *hstreampb.RecordId, pb *hstreampb.HStreamRecord) (*RawRecord, error) {
	header, err := RecordHeaderFromPb(pb.GetHeader())
	if err != nil {
		return nil, err
	}
	return &RawRecord{
		Header:   header,
		RecordId: RecordIdFromPb(rid),
		Payload:  pb.GetPayload(),
	}, nil
}

func fromRPCHRecord(rid *hstreampb.RecordId, pb *hstreampb.HStreamRecord) (*HRecord, error) {
	hRecord := &HRecord{}
	if err := json.Unmarshal(pb.GetPayload(), &hRecord.Payload); err != nil {
		return hRecord, errors.Wrap(err, "failed to unmarshal hrecord")
	}
	hRecord.RecordId = RecordIdFromPb(rid)
	header, err := RecordHeaderFromPb(pb.GetHeader())
	if err != nil {
		return nil, err
	}
	hRecord.Header = header
	return hRecord, nil
}

func StatsIntervalsToPb(intervals []int32) *hstreampb.StatsIntervalVals {
	return &hstreampb.StatsIntervalVals{
		Intervals: intervals,
	}
}

func StatsFromPb(stats *hstreampb.StatsDoubleVals) (*Stats, error) {
	stats.GetVals()
	return &Stats{
		Values: stats.GetVals(),
	}, nil
}

func SubscriptionOffsetToPb(offset SubscriptionOffset) hstreampb.SpecialOffset {
	var res hstreampb.SpecialOffset
	switch offset {
	case EARLIEST:
		res = hstreampb.SpecialOffset_EARLIEST
	case LATEST:
		res = hstreampb.SpecialOffset_LATEST
	default:
		util.Logger().Fatal("Unknown offset")
	}
	return res
}

func SubscriptionOffsetFromPb(offset hstreampb.SpecialOffset) SubscriptionOffset {
	var res SubscriptionOffset
	switch offset {
	case hstreampb.SpecialOffset_EARLIEST:
		res = EARLIEST
	case hstreampb.SpecialOffset_LATEST:
		res = LATEST
	default:
		util.Logger().Fatal("Unknown offset")
	}
	return res
}
