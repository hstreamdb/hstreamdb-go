package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

type StatsType interface {
	stats()
}

type StreamStatsType int

const (
	StreamAppendInBytes StreamStatsType = iota + 1
	StreamAppendInRecords
	TotalAppend
	FailedAppend
	StreamAppendRequestCnt
)

func (s StreamStatsType) String() string {
	switch s {
	case StreamAppendInBytes:
		return "StreamAppendInBytes"
	case StreamAppendInRecords:
		return "StreamAppendInRecords"
	case TotalAppend:
		return "TotalAppend"
	case FailedAppend:
		return "FailedAppend"
	}
	return ""
}

func (s StreamStatsType) stats() {}

func (c *HStreamClient) GetStreamStatsRequest(addr string, statsType StreamStatsType) (map[string]int64, error) {
	var (
		resp *hstreamrpc.Response
		err  error
	)

	req := &hstreamrpc.Request{
		Type: hstreamrpc.GetStreamStatsRequest,
		Req: &hstreampb.GetStreamStatsRequest{
			StatsType: StreamStatsTypeToPb(statsType),
		},
	}

	if resp, err = c.sendRequest(addr, req); err != nil {
		return nil, err
	}
	response := resp.Resp.(*hstreampb.GetStreamStatsResponse).GetStatValues()
	return response, nil
}

type SubscriptionStatsType int

const (
	SubDeliveryInBytes SubscriptionStatsType = iota + 1
	SubDeliveryInRecords
	AckReceived
	ResendRecords
	SubMessageRequestCnt
	SubMessageResponseCnt
)

func (s SubscriptionStatsType) String() string {
	switch s {
	case SubDeliveryInBytes:
		return "SubDeliveryInBytes"
	case SubDeliveryInRecords:
		return "SubDeliveryInRecords"
	case AckReceived:
		return "AckReceived"
	case ResendRecords:
		return "ResendRecords"
	case SubMessageRequestCnt:
		return "SubMessageRequestCnt"
	case SubMessageResponseCnt:
		return "SubMessageResponseCnt"
	}
	return ""
}

func (s SubscriptionStatsType) stats() {}

func (c *HStreamClient) GetSubscriptionStatsRequest(addr string, statsType SubscriptionStatsType) (map[string]int64, error) {
	var (
		resp *hstreamrpc.Response
		err  error
	)

	req := &hstreamrpc.Request{
		Type: hstreamrpc.GetSubscriptionStatsRequest,
		Req: &hstreampb.GetSubscriptionStatsRequest{
			StatsType: SubscriptionStatsTypeToPb(statsType),
		},
	}

	if resp, err = c.sendRequest(addr, req); err != nil {
		return nil, err
	}
	response := resp.Resp.(*hstreampb.GetSubscriptionStatsResponse).GetStatValues()
	return response, nil
}
