package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/internal/hstreamrpc"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamdb/hstream/server"
)

type Stats struct {
	Values []float64
}

// AdminRequestToRandomServer send admin command request to a random server in the cluster
func (c *HStreamClient) AdminRequestToRandomServer(cmd string) (string, error) {
	var (
		resp *hstreamrpc.Response
		err  error
	)

	req := &hstreamrpc.Request{
		Type: hstreamrpc.AdminRequest,
		Req: &hstreampb.AdminCommandRequest{
			Command: cmd,
		},
	}

	if resp, err = c.reqToRandomServer(req); err != nil {
		return "", err
	}
	response := resp.Resp.(*hstreampb.AdminCommandResponse).GetResult()
	return response, nil
}

func (c *HStreamClient) StreamStatsRequest(method, streamName string, intervals []int32) (*Stats, error) {
	var (
		resp *hstreamrpc.Response
		err  error
	)

	req := &hstreamrpc.Request{
		Type: hstreamrpc.PerStreamStats,
		Req: &hstreampb.PerStreamTimeSeriesStatsRequest{
			Method:     method,
			StreamName: streamName,
			Intervals:  StatsIntervalsToPb(intervals),
		},
	}

	if resp, err = c.reqToRandomServer(req); err != nil {
		return nil, err
	}
	stat := resp.Resp.(*hstreampb.PerStreamTimeSeriesStatsResponse).GetStats()
	return StatsFromPb(stat)
}

func (c *HStreamClient) StreamStatsAllRequest(method string, intervals []int32) (map[string]*Stats, error) {
	var (
		resp *hstreamrpc.Response
		err  error
	)

	req := &hstreamrpc.Request{
		Type: hstreamrpc.PerStreamStatsAll,
		Req: &hstreampb.PerStreamTimeSeriesStatsAllRequest{
			Method:    method,
			Intervals: StatsIntervalsToPb(intervals),
		},
	}

	if resp, err = c.reqToRandomServer(req); err != nil {
		return nil, err
	}
	stats := resp.Resp.(*hstreampb.PerStreamTimeSeriesStatsAllResponse).GetStats()
	res := make(map[string]*Stats, len(stats))
	for key, value := range stats {
		stat, _ := StatsFromPb(value)
		res[key] = stat
	}
	return res, nil
}
