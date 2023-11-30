package ticker

import (
	"sync"
	"time"
)

type DynamicTicker struct {
	C               <-chan time.Time
	stop            chan struct{}
	setDuration     chan time.Duration
	revertToInitial chan struct{}
	wg              sync.WaitGroup
	initialDuration time.Duration
	currentDuration time.Duration
	stopped         bool
	durationMutex   sync.RWMutex
	stoppedMutex    sync.Mutex
}

// NewDynamicTicker creates a new DynamicTicker with a specified duration.
func NewDynamicTicker(d time.Duration) *DynamicTicker {
	ticker := &DynamicTicker{
		stop:            make(chan struct{}),
		setDuration:     make(chan time.Duration, 1),
		revertToInitial: make(chan struct{}, 1),
		initialDuration: d,
		currentDuration: d,
		stopped:         false,
	}

	ch := make(chan time.Time)
	ticker.C = ch

	ticker.wg.Add(1)
	go ticker.runTicker(ch)

	return ticker
}

// runTicker manages the internal ticker.
func (t *DynamicTicker) runTicker(ch chan<- time.Time) {
	defer t.wg.Done()

	d := t.initialDuration
	timer := time.NewTimer(0) // Fire immediately

	for {
		select {
		case <-timer.C:
			ch <- time.Now()
			timer.Reset(d)
		case newDuration := <-t.setDuration:
			d = newDuration
			t.durationMutex.Lock()
			t.currentDuration = d
			t.durationMutex.Unlock()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		case <-t.revertToInitial:
			d = t.initialDuration
			t.durationMutex.Lock()
			t.currentDuration = d
			t.durationMutex.Unlock()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		case <-t.stop:
			timer.Stop()
			t.stoppedMutex.Lock()
			t.stopped = true
			t.stoppedMutex.Unlock()
			return
		}
	}
}

// Stop stops the DynamicTicker.
func (t *DynamicTicker) Stop() {
	close(t.stop)
	t.wg.Wait()
}

// SetInterval sets a new interval for the DynamicTicker.
func (t *DynamicTicker) SetInterval(d time.Duration) {
	t.stoppedMutex.Lock()
	if t.stopped {
		t.stoppedMutex.Unlock()
		return
	}
	t.stoppedMutex.Unlock()

	select {
	case t.setDuration <- d:
	default:
	}
}

// SetIntervalWithRevert sets a new interval for the DynamicTicker and reverts to the initial duration after a specified time.
func (t *DynamicTicker) SetIntervalWithRevert(newInterval, revertAfter time.Duration) {
	t.SetInterval(newInterval)

	go func() {
		time.Sleep(revertAfter)
		select {
		case t.revertToInitial <- struct{}{}:
		default:
		}
	}()
}

// CurrentDuration returns the current duration of the DynamicTicker.
func (t *DynamicTicker) CurrentDuration() time.Duration {
	t.durationMutex.RLock()
	defer t.durationMutex.RUnlock()
	return t.currentDuration
}
