package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

type StatType interface {
	toPbStat() (tp *hstreampb.StatType)
	String() string
}

type StreamStatsType int

const (
	StreamAppendInBytes StreamStatsType = iota + 1
	StreamAppendInRecords
	StreamAppendTotal
	StreamAppendFailed
)

func (s StreamStatsType) String() string {
	switch s {
	case StreamAppendInBytes:
		return "StreamAppendInBytes"
	case StreamAppendInRecords:
		return "StreamAppendInRecords"
	case StreamAppendTotal:
		return "StreamAppendTotal"
	case StreamAppendFailed:
		return "StreamAppendFailed"
	}
	return ""
}

func (s StreamStatsType) toPbStat() *hstreampb.StatType {
	var tp *hstreampb.StatType_StreamStat
	switch s {
	case StreamAppendInBytes:
		tp = &hstreampb.StatType_StreamStat{StreamStat: hstreampb.StreamStats_AppendInBytes}
	case StreamAppendInRecords:
		tp = &hstreampb.StatType_StreamStat{StreamStat: hstreampb.StreamStats_AppendInRecords}
	case StreamAppendTotal:
		tp = &hstreampb.StatType_StreamStat{StreamStat: hstreampb.StreamStats_AppendTotal}
	case StreamAppendFailed:
		tp = &hstreampb.StatType_StreamStat{StreamStat: hstreampb.StreamStats_AppendFailed}
	}
	return &hstreampb.StatType{Stat: tp}
}

type SubscriptionStatsType int

const (
	SubSendOutBytes SubscriptionStatsType = iota + 1
	SubSendOutRecords
	SubSendOutRecordsFailed
	SubResendRecords
	SubResendRecordsFailed
	ReceivedAcks
	SubRequestMessages
	SubResponseMessages
)

func (s SubscriptionStatsType) String() string {
	switch s {
	case SubSendOutBytes:
		return "SubSendOutBytes"
	case SubSendOutRecords:
		return "SubSendOutRecords"
	case SubSendOutRecordsFailed:
		return "SubSendOutRecordsFailed"
	case SubResendRecords:
		return "SubResendRecords"
	case SubResendRecordsFailed:
		return "SubResendRecordsFailed"
	case ReceivedAcks:
		return "ReceivedAcks"
	case SubRequestMessages:
		return "SubRequestMessages"
	case SubResponseMessages:
		return "SubResponseMessages"
	}
	return ""
}

func (s SubscriptionStatsType) toPbStat() *hstreampb.StatType {
	var tp *hstreampb.StatType_SubStat
	switch s {
	case SubSendOutBytes:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_SendOutBytes}
	case SubSendOutRecords:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_SendOutRecords}
	case SubSendOutRecordsFailed:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_SendOutRecordsFailed}
	case SubResendRecords:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_ResendRecords}
	case SubResendRecordsFailed:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_ResendRecordsFailed}
	case ReceivedAcks:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_ReceivedAcks}
	case SubRequestMessages:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_RequestMessages}
	case SubResponseMessages:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_ResponseMessages}
	}
	return &hstreampb.StatType{Stat: tp}
}

type StatResult interface {
	statResult()
}

type StatValue struct {
	Type  StatType
	Value map[string]int64
}

func (s StatValue) statResult() {}

type StatError struct {
	Type    StatType
	Message string
}

func (s StatError) statResult() {}

func (c *HStreamClient) GetStatsRequest(addr string, statsTypes []StatType) ([]StatResult, error) {
	var (
		resp *hstreamrpc.Response
		err  error
	)

	states := make([]*hstreampb.StatType, 0, len(statsTypes))
	for _, st := range statsTypes {
		states = append(states, st.toPbStat())
	}
	req := &hstreamrpc.Request{
		Type: hstreamrpc.GetStatsRequest,
		Req: &hstreampb.GetStatsRequest{
			Stats: states,
		},
	}

	if resp, err = c.sendRequest(addr, req); err != nil {
		return nil, err
	}
	response := resp.Resp.(*hstreampb.GetStatsResponse)
	success := response.GetStatsValues()
	failed := response.GetErrors()
	res := make([]StatResult, 0, len(success)+len(failed))
	for _, s := range success {
		res = append(res, StatValueFromPb(s))
	}
	for _, e := range failed {
		res = append(res, StatErrorFromPb(e))
	}
	return res, nil
}
