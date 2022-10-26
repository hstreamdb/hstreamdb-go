package hstream

import (
	"github.com/hstreamdb/hstreamdb-go/util"
	"go.uber.org/zap"
	"sync"
)

type flowController struct {
	cond                *sync.Cond
	mu                  *sync.Mutex
	outStandingBytes    uint64
	maxOutStandingBytes uint64
	waitingList         []uint64
}

func newFlowController(maxBytes uint64) *flowController {
	lk := &sync.Mutex{}
	return &flowController{
		cond:                sync.NewCond(lk),
		mu:                  lk,
		outStandingBytes:    maxBytes,
		maxOutStandingBytes: maxBytes,
	}
}

func (f *flowController) Acquire(need uint64) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	if f.outStandingBytes >= need {
		f.outStandingBytes -= need
		util.Logger().Debug("acquire bytes success", zap.Int("need", int(need)), zap.Int("remained", int(f.outStandingBytes)))
		return
	}

	util.Logger().Debug("wait", zap.Int("need", int(need)), zap.Int("remained", int(f.outStandingBytes)))
	f.waitingList = append(f.waitingList, need)
	for len(f.waitingList) != 0 && f.outStandingBytes < f.waitingList[0] {
		f.cond.Wait()
	}
	f.outStandingBytes -= need
	f.waitingList = f.waitingList[1:]
}

func (f *flowController) Release(release uint64) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	f.outStandingBytes += release
	if f.outStandingBytes > f.maxOutStandingBytes {
		f.outStandingBytes = f.maxOutStandingBytes
	}
	util.Logger().Debug("release bytes", zap.Int("need", int(release)), zap.Int("remained", int(f.outStandingBytes)))
	f.cond.Signal()
}
