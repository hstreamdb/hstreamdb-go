package test_util

import (
	"github.com/hstreamdb/hstreamdb-go/internal/client"
	hstreampb "github.com/hstreamdb/hstreamdb-go/proto/gen-proto/hstreamDB/hstream/server"
)

var ServerUrl = "localhost:6580,localhost:6581,localhost:6582"

type GatherRidsHandler struct {
	res     []*hstreampb.RecordId
	maxSize int
	Err     error
	Done    chan struct{}
}

func MakeGatherRidsHandler(size int) *GatherRidsHandler {
	return &GatherRidsHandler{
		maxSize: size,
		res:     make([]*hstreampb.RecordId, 0, size),
		Done:    make(chan struct{}),
	}
}

func (h *GatherRidsHandler) HandleRes(res client.FetchResult) {
	records, err := res.GetResult()
	if err != nil {
		h.Err = err
		return
	}

	for _, record := range records {
		h.res = append(h.res, record.GetRecordId())
		if len(h.res) >= h.maxSize {
			break
		}
	}
	h.Done <- struct{}{}
}

func (h *GatherRidsHandler) GetRes() []*hstreampb.RecordId {
	return h.res
}
