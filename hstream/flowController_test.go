package hstream

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestFlowController(t *testing.T) {
	t.Parallel()
	controller := newFlowController(1024 * 8)
	wg := sync.WaitGroup{}
	wg.Add(12)
	for i := 0; i < 12; i++ {
		go func() {
			defer wg.Done()
			controller.Acquire(1024)
		}()
	}
	time.Sleep(1 * time.Second)

	controller.mu.Lock()
	require.Equal(t, []uint64{1024, 1024, 1024, 1024}, controller.waitingList)
	controller.mu.Unlock()

	for i := 0; i < 4; i++ {
		controller.Release(1024)
	}
	wg.Wait()
	require.True(t, len(controller.waitingList) == 0)
}
