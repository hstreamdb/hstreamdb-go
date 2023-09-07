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
	StreamReadInBytes
	StreamReadInBatches
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
	case StreamReadInBytes:
		return "StreamReadInBytes"
	case StreamReadInBatches:
		return "StreamReadInBatches"
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
	case StreamReadInBytes:
		tp = &hstreampb.StatType_StreamStat{StreamStat: hstreampb.StreamStats_ReadInBytes}
	case StreamReadInBatches:
		tp = &hstreampb.StatType_StreamStat{StreamStat: hstreampb.StreamStats_ReadInBatches}
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
	SubCheckListSize
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
	case SubCheckListSize:
		return "SubCheckListSize"
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
	case SubCheckListSize:
		tp = &hstreampb.StatType_SubStat{SubStat: hstreampb.SubscriptionStats_ChecklistSize}
	}
	return &hstreampb.StatType{Stat: tp}
}

type QueryStatsType int

const (
	QueryTotalInputRecords QueryStatsType = iota + 1
	QueryTotalOutputRecords
	QueryTotalExcuteErrors
)

func (q QueryStatsType) String() string {
	switch q {
	case QueryTotalInputRecords:
		return "QueryTotalInputRecords"
	case QueryTotalOutputRecords:
		return "QueryTotalOutputRecords"
	case QueryTotalExcuteErrors:
		return "QueryTotalExcuteErrors"
	}
	return ""
}

func (q QueryStatsType) toPbStat() *hstreampb.StatType {
	var tp *hstreampb.StatType_QueryStat
	switch q {
	case QueryTotalInputRecords:
		tp = &hstreampb.StatType_QueryStat{QueryStat: hstreampb.QueryStats_TotalInputRecords}
	case QueryTotalOutputRecords:
		tp = &hstreampb.StatType_QueryStat{QueryStat: hstreampb.QueryStats_TotalOutputRecords}
	case QueryTotalExcuteErrors:
		tp = &hstreampb.StatType_QueryStat{QueryStat: hstreampb.QueryStats_TotalExecuteErrors}
	}
	return &hstreampb.StatType{Stat: tp}
}

type ViewStatsType int

const (
	ViewTotalExecuteQueries ViewStatsType = iota + 1
)

func (v ViewStatsType) String() string {
	switch v {
	case ViewTotalExecuteQueries:
		return "ViewTotalExecuteQueries"
	}
	return ""
}

func (v ViewStatsType) toPbStat() *hstreampb.StatType {
	var tp *hstreampb.StatType_ViewStat
	switch v {
	case ViewTotalExecuteQueries:
		tp = &hstreampb.StatType_ViewStat{ViewStat: hstreampb.ViewStats_TotalExecuteQueries}
	}
	return &hstreampb.StatType{Stat: tp}
}

type ConnectorStatsType int

const (
	ConnectorDeliveredInRecords ConnectorStatsType = iota + 1
	ConnectorDeliveredInBytes
	ConnectorIsAlive
)

func (c ConnectorStatsType) String() string {
	switch c {
	case ConnectorDeliveredInRecords:
		return "ConnectorDeliveredInRecords"
	case ConnectorDeliveredInBytes:
		return "ConnectorDeliveredInBytes"
	case ConnectorIsAlive:
		return "ConnectorIsAlive"
	}
	return ""
}

func (c ConnectorStatsType) toPbStat() *hstreampb.StatType {
	var tp *hstreampb.StatType_ConnStat
	switch c {
	case ConnectorDeliveredInRecords:
		tp = &hstreampb.StatType_ConnStat{ConnStat: hstreampb.ConnectorStats_DeliveredInRecords}
	case ConnectorDeliveredInBytes:
		tp = &hstreampb.StatType_ConnStat{ConnStat: hstreampb.ConnectorStats_DeliveredInBytes}
	case ConnectorIsAlive:
		tp = &hstreampb.StatType_ConnStat{ConnStat: hstreampb.ConnectorStats_IsAlive}
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
