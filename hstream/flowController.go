package hstream

import (
	"sync"
)

type flowController struct {
	cond             *sync.Cond
	outStandingBytes uint64
	waitingList      []uint64
}

func newFlowController(maxBytes uint64) *flowController {
	return &flowController{
		cond:             sync.NewCond(&sync.Mutex{}),
		outStandingBytes: maxBytes,
	}
}

func (f *flowController) Acquire(need uint64) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	if f.outStandingBytes >= need {
		f.outStandingBytes -= need
		//util.Logger().Info("acquire bytes success", zap.Int("need", int(need)), zap.Int("remained", int(f.outStandingBytes)))
		return
	}

	//util.Logger().Info("wait", zap.Int("need", int(need)), zap.Int("remained", int(f.outStandingBytes)))
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
	//util.Logger().Info("release bytes", zap.Int("need", int(release)), zap.Int("remained", int(f.outStandingBytes)))
	f.cond.Signal()
}
