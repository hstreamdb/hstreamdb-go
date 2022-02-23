package test_util

import (
	"client/client"
	hstreampb "client/gen-proto/hstream/server"
)

var ServerUrl = "localhost:6580,localhost:6581,localhost:6582"

type GatherRidsHandler struct {
	res []*hstreampb.RecordId
	Err error
}

func MakeGatherRidsHandler(size int) *GatherRidsHandler {
	return &GatherRidsHandler{
		res: make([]*hstreampb.RecordId, 0, size),
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
	}
}

func (h *GatherRidsHandler) GetRes() []*hstreampb.RecordId {
	return h.res
}
