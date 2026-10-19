package streamhub

import (
	"context"
	"math"
	"sync"
)

// providerSlot is the provider connection lock. When it frees up, it goes to the waiter whose
// viewers are closest to running out of buffer (smaller urgency = more urgent). We read the
// urgency at the moment the lock frees up, not when the wait began, because the buffer runs
// down while it waits. On a tie, whoever waited longer gets it.
type providerSlot struct {
	mu      sync.Mutex
	busy    bool
	waiting []*slotWaiter
	seq     uint64
}

type slotWaiter struct {
	turn    chan struct{}
	seq     uint64         // arrival order, for ties
	urgency func() float64 // seconds of stream left for the viewer closest to stopping
}

// fixed urgency values:
const (
	urgentNow   = 0.0 // new channel: black screen
	urgentLater = 1e6 // running channel without PCR: in arrival order
)

func newProviderSlot() *providerSlot {
	return &providerSlot{}
}

// acquireBy waits for the turn by urgency; false if ctx is done before it comes.
func (s *providerSlot) acquireBy(ctx context.Context, urgency func() float64) bool {
	s.mu.Lock()
	if !s.busy {
		s.busy = true
		s.mu.Unlock()
		return true
	}
	s.seq++
	w := &slotWaiter{turn: make(chan struct{}), seq: s.seq, urgency: urgency}
	s.waiting = append(s.waiting, w)
	s.mu.Unlock()

	select {
	case <-w.turn:
		return true
	case <-ctx.Done():
		s.mu.Lock()
		waiting := s.dequeue(w)
		s.mu.Unlock()
		if !waiting {
			// the turn came at the very moment we gave up: pass it on to the next one
			s.release()
		}
		return false
	}
}

// release gives the turn to the most urgent waiter, else frees the lock.
func (s *providerSlot) release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.waiting) == 0 {
		s.busy = false
		return
	}
	best, bestU := 0, math.Inf(1)
	for i, w := range s.waiting {
		u := w.urgency()
		if u < bestU || (u == bestU && w.seq < s.waiting[best].seq) {
			best, bestU = i, u
		}
	}
	w := s.waiting[best]
	s.waiting = append(s.waiting[:best], s.waiting[best+1:]...)
	close(w.turn) // busy stays true: the lock moved to whoever's turn it is
}

// dequeue removes w from the queue if it is still waiting; the caller holds s.mu.
func (s *providerSlot) dequeue(w *slotWaiter) bool {
	for i, x := range s.waiting {
		if x == w {
			s.waiting = append(s.waiting[:i], s.waiting[i+1:]...)
			return true
		}
	}
	return false
}
