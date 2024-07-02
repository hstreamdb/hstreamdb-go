package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/hstream/Record"
	"github.com/hstreamdb/hstreamdb-go/hstream/compression"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
	"github.com/hstreamdb/hstreamdb-go/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func RecordIdToPb(r Record.RecordId) *hstreampb.RecordId {
	return &hstreampb.RecordId{
		BatchId:    r.BatchId,
		BatchIndex: r.BatchIndex,
		ShardId:    r.ShardId,
	}
}

func RecordIdFromPb(pb *hstreampb.RecordId) Record.RecordId {
	return Record.RecordId{
		BatchId:    pb.BatchId,
		BatchIndex: pb.BatchIndex,
		ShardId:    pb.ShardId,
	}
}

func ReceivedRecordFromPb(record *hstreampb.HStreamRecord, rid *hstreampb.RecordId) (Record.ReceivedRecord, error) {
	switch record.GetHeader().GetFlag() {
	case hstreampb.HStreamRecordHeader_RAW:
		return FromPbRawRecord(rid, record)
	case hstreampb.HStreamRecordHeader_JSON:
		return FromPbHRecord(rid, record)
	default:
		return nil, errors.Errorf("unknown record type: %s", record.GetHeader().GetFlag())
	}
}

func FromPbRawRecord(rid *hstreampb.RecordId, pb *hstreampb.HStreamRecord) (*Record.ReceivedRawRecord, error) {
	header, err := RecordHeaderFromPb(pb.GetHeader())
	if err != nil {
		return nil, err
	}
	return &Record.ReceivedRawRecord{
		Header:   header,
		RecordId: RecordIdFromPb(rid),
		Payload:  pb.GetPayload(),
	}, nil
}

func FromPbHRecord(rid *hstreampb.RecordId, pb *hstreampb.HStreamRecord) (*Record.ReceivedHRecord, error) {
	hRecord := &Record.ReceivedHRecord{}
	res := &structpb.Struct{}
	if err := proto.Unmarshal(pb.GetPayload(), res); err != nil {
		return hRecord, errors.Wrap(err, "failed to unmarshal hrecord")
	}
	hRecord.RecordId = RecordIdFromPb(rid)
	header, err := RecordHeaderFromPb(pb.GetHeader())
	if err != nil {
		return nil, err
	}
	hRecord.Header = header
	hRecord.Payload = res
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

func ShardFromPb(pbShard *hstreampb.Shard) Shard {
	return Shard{
		ShardId:      pbShard.GetShardId(),
		StreamName:   pbShard.GetStreamName(),
		StartHashKey: pbShard.GetStartHashRangeKey(),
		EndHashKey:   pbShard.GetEndHashRangeKey(),
	}
}

func ShardToPb(shard *Shard) *hstreampb.Shard {
	return &hstreampb.Shard{
		ShardId:           shard.ShardId,
		StreamName:        shard.StreamName,
		StartHashRangeKey: shard.StartHashKey,
		EndHashRangeKey:   shard.EndHashKey,
	}
}

func HStreamRecordToPb(r Record.HStreamRecord) (*hstreampb.HStreamRecord, error) {
	switch record := r.(type) {
	case *Record.RawRecord:
		return &hstreampb.HStreamRecord{
			Header: &hstreampb.HStreamRecordHeader{
				Key:  r.GetKey(),
				Flag: hstreampb.HStreamRecordHeader_RAW,
			},
			Payload: record.Payload,
		}, nil
	case *Record.HRecord:
		payload, err := proto.Marshal(record.Payload)
		if err != nil {
			return nil, err
		}
		return &hstreampb.HStreamRecord{
			Header: &hstreampb.HStreamRecordHeader{
				Key:  r.GetKey(),
				Flag: hstreampb.HStreamRecordHeader_JSON,
			},
			Payload: payload,
		}, nil
	}
	return nil, nil
}

// func recordHeaderToPb(r *Record.RecordHeader) *hstreampb.HStreamRecordHeader {
//	pb := &hstreampb.HStreamRecordHeader{
//		Key:  r.Key,
//		Flag: Record.RecordTypeToPb(r.Flag),
//	}
//	if len(r.Attributes) > 0 {
//		pb.Attributes = make(map[string]string)
//		for k, v := range r.Attributes {
//			pb.Attributes[k] = v
//		}
//	}
//	return pb
//}

func RecordHeaderFromPb(pb *hstreampb.HStreamRecordHeader) (Record.RecordHeader, error) {
	flag, err := RecordTypeFromPb(pb.GetFlag())
	if err != nil {
		util.Logger().Error("failed to parse record type: %s", zap.Error(err))
		return Record.RecordHeader{}, err
	}
	return Record.RecordHeader{
		Key:        pb.GetKey(),
		Flag:       flag,
		Attributes: pb.GetAttributes(),
	}, nil
}

func RecordTypeFromPb(pb hstreampb.HStreamRecordHeader_Flag) (Record.RecordType, error) {
	switch pb {
	case hstreampb.HStreamRecordHeader_RAW:
		return Record.RAWRECORD, nil
	case hstreampb.HStreamRecordHeader_JSON:
		return Record.HRECORD, nil
	default:
		return Record.UNKNOWN, errors.Errorf("unknown record type: %s", pb)
	}
}

func RecordTypeToPb(r Record.RecordType) (flag hstreampb.HStreamRecordHeader_Flag) {
	switch r {
	case Record.RAWRECORD:
		flag = hstreampb.HStreamRecordHeader_RAW
	case Record.HRECORD:
		flag = hstreampb.HStreamRecordHeader_JSON
	}
	return
}

func CompressionTypeToPb(c compression.CompressionType) (tp hstreampb.CompressionType) {
	switch c {
	case compression.None:
		tp = hstreampb.CompressionType_None
	case compression.Gzip:
		tp = hstreampb.CompressionType_Gzip
	case compression.Zstd:
		tp = hstreampb.CompressionType_Zstd
	}
	return
}

func CompressionTypeFromPb(c hstreampb.CompressionType) (tp compression.CompressionType) {
	switch c {
	case hstreampb.CompressionType_None:
		tp = compression.None
	case hstreampb.CompressionType_Gzip:
		tp = compression.Gzip
	case hstreampb.CompressionType_Zstd:
		tp = compression.Zstd
	}
	return
}

func StatValueFromPb(pb *hstreampb.StatValue) StatValue {
	return StatValue{
		Type:  StatsTypeFromPb(pb.GetStatType()),
		Value: pb.GetStatValues(),
	}
}

func StatErrorFromPb(pb *hstreampb.StatError) StatError {
	return StatError{
		Type:    StatsTypeFromPb(pb.GetStatType()),
		Message: pb.GetMessage(),
	}
}

func StatsTypeFromPb(tp *hstreampb.StatType) StatType {
	statTp := tp.GetStat()
	switch st := statTp.(type) {
	case *hstreampb.StatType_StreamStat:
		return StreamStatsTypeFromPb(st)
	case *hstreampb.StatType_SubStat:
		return SubStatsTypeFromPb(st)
	case *hstreampb.StatType_ConnStat:
		return ConnStatsTypeFromPb(st)
	case *hstreampb.StatType_QueryStat:
		return QueryStatsTypeFromPb(st)
	case *hstreampb.StatType_ViewStat:
		return ViewStatsTypeFromPb(st)
	case *hstreampb.StatType_CacheStoreStat:
		return CacheStoreStatsTypeFromPb(st)
	}
	return nil
}

func ConnStatsTypeFromPb(tp *hstreampb.StatType_ConnStat) (t ConnectorStatsType) {
	switch tp.ConnStat {
	case hstreampb.ConnectorStats_DeliveredInRecords:
		t = ConnectorDeliveredInRecords
	case hstreampb.ConnectorStats_DeliveredInBytes:
		t = ConnectorDeliveredInBytes
	case hstreampb.ConnectorStats_IsAlive:
		t = ConnectorIsAlive
	}
	return t
}

func StreamStatsTypeFromPb(tp *hstreampb.StatType_StreamStat) (t StreamStatsType) {
	switch tp.StreamStat {
	case hstreampb.StreamStats_AppendInBytes:
		t = StreamAppendInBytes
	case hstreampb.StreamStats_AppendInRecords:
		t = StreamAppendInRecords
	case hstreampb.StreamStats_AppendTotal:
		t = StreamAppendTotal
	case hstreampb.StreamStats_AppendFailed:
		t = StreamAppendFailed
	case hstreampb.StreamStats_ReadInBytes:
		t = StreamReadInBytes
	case hstreampb.StreamStats_ReadInBatches:
		t = StreamReadInBatches
	}
	return t
}

func CacheStoreStatsTypeFromPb(tp *hstreampb.StatType_CacheStoreStat) (t CacheStoreStatsType) {
	switch tp.CacheStoreStat {
	case hstreampb.CacheStoreStats_CSAppendInBytes:
		t = CacheStoreAppendInBytes
	case hstreampb.CacheStoreStats_CSAppendInRecords:
		t = CacheStoreAppendInRecords
	case hstreampb.CacheStoreStats_CSAppendTotal:
		t = CacheStoreAppendTotal
	case hstreampb.CacheStoreStats_CSAppendFailed:
		t = CacheStoreAppendFailed
	case hstreampb.CacheStoreStats_CSReadInBytes:
		t = CacheStoreReadInBytes
	case hstreampb.CacheStoreStats_CSReadInRecords:
		t = CacheStoreReadInRecords
	case hstreampb.CacheStoreStats_CSDeliveredInRecords:
		t = CacheStoreDeliveredInRecords
	}
	return t
}

func SubStatsTypeFromPb(tp *hstreampb.StatType_SubStat) (t SubscriptionStatsType) {
	switch tp.SubStat {
	case hstreampb.SubscriptionStats_SendOutBytes:
		t = SubSendOutBytes
	case hstreampb.SubscriptionStats_SendOutRecords:
		t = SubSendOutRecords
	case hstreampb.SubscriptionStats_SendOutRecordsFailed:
		t = SubSendOutRecordsFailed
	case hstreampb.SubscriptionStats_ResendRecords:
		t = SubResendRecords
	case hstreampb.SubscriptionStats_ResendRecordsFailed:
		t = SubResendRecordsFailed
	case hstreampb.SubscriptionStats_ReceivedAcks:
		t = ReceivedAcks
	case hstreampb.SubscriptionStats_RequestMessages:
		t = SubRequestMessages
	case hstreampb.SubscriptionStats_ResponseMessages:
		t = SubResponseMessages
	case hstreampb.SubscriptionStats_ChecklistSize:
		t = SubCheckListSize
	}
	return t
}

func QueryStatsTypeFromPb(tp *hstreampb.StatType_QueryStat) (t QueryStatsType) {
	switch tp.QueryStat {
	case hstreampb.QueryStats_TotalInputRecords:
		t = QueryTotalInputRecords
	case hstreampb.QueryStats_TotalOutputRecords:
		t = QueryTotalOutputRecords
	case hstreampb.QueryStats_TotalExecuteErrors:
		t = QueryTotalExcuteErrors
	}
	return t
}

func ViewStatsTypeFromPb(tp *hstreampb.StatType_ViewStat) (t ViewStatsType) {
	switch tp.ViewStat {
	case hstreampb.ViewStats_TotalExecuteQueries:
		t = ViewTotalExecuteQueries
	}
	return t
}
