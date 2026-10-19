package streamhub

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// Most tests open channels that never send anything, and each would hold the
// provider slot for the whole SlotHold; keep it short so they don't wait on it.
func TestMain(m *testing.M) {
	SlotHold = 100 * time.Millisecond
	os.Exit(m.Run())
}

func withSlotHold(t *testing.T, d time.Duration) {
	old := SlotHold
	SlotHold = d
	t.Cleanup(func() { SlotHold = old })
}

// openedWithin reports whether src opened a connection within d.
func openedWithin(src *pipeSource, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if src.opens.Load() > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// Only one channel connects at a time: a second channel waits until the first
// has its data, so it does not cut the first off in the middle of it.
func TestOnlyOneChannelConnectsAtATime(t *testing.T) {
	withSlotHold(t, 5*time.Second)
	h := NewHub()
	ctx := context.Background()

	srcA := newPipeSource()
	a, err := h.Subscribe(ctx, "live/a.ts", srcA.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(a)
	wA := <-srcA.w

	srcB := newPipeSource()
	go func() {
		if b, err := h.Subscribe(ctx, "live/b.ts", srcB.open); err == nil {
			t.Cleanup(func() { h.Unsubscribe(b) })
		}
	}()

	if openedWithin(srcB, 200*time.Millisecond) {
		t.Fatal("the second channel connected while the first was still waiting for its data")
	}
	wA.Write([]byte("first data")) // channel A has its data: the slot is free
	if !openedWithin(srcB, 2*time.Second) {
		t.Fatal("the second channel never connected after the first got its data")
	}
	if got := recv(t, a); got != "first data" {
		t.Errorf("viewer of the first channel got %q", got)
	}
}

// A channel whose provider accepts the connection but sends nothing gives the
// slot up after SlotHold, so it cannot block every other channel.
func TestSilentChannelGivesUpTheSlot(t *testing.T) {
	withSlotHold(t, 100*time.Millisecond)
	h := NewHub()
	ctx := context.Background()

	srcA := newPipeSource()
	a, _ := h.Subscribe(ctx, "live/a.ts", srcA.open)
	defer h.Unsubscribe(a)

	srcB := newPipeSource()
	go func() {
		if b, err := h.Subscribe(ctx, "live/b.ts", srcB.open); err == nil {
			t.Cleanup(func() { h.Unsubscribe(b) })
		}
	}()
	if !openedWithin(srcB, 2*time.Second) {
		t.Fatal("a silent channel kept the slot")
	}
}

// A channel closed while it waits for the slot stops waiting and never connects.
func TestClosedChannelStopsWaitingForTheSlot(t *testing.T) {
	withSlotHold(t, 5*time.Second)
	h := NewHub()

	srcA := newPipeSource()
	a, _ := h.Subscribe(context.Background(), "live/a.ts", srcA.open)
	defer h.Unsubscribe(a)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	srcB := newPipeSource()
	if _, err := h.Subscribe(ctx, "live/b.ts", srcB.open); err == nil {
		t.Fatal("subscribing while the slot was taken succeeded before the timeout")
	}
	time.Sleep(IdleTimeout + 200*time.Millisecond) // the abandoned channel closes
	if n := srcB.opens.Load(); n != 0 {
		t.Errorf("the abandoned channel connected anyway (%d opens)", n)
	}
}

// waitTurn starts a goroutine that waits for the slot and reports who got it.
func waitTurn(s *providerSlot, ctx context.Context, urgent bool, name string, got chan<- string) {
	go func() {
		if s.acquire(ctx, urgent) {
			got <- name
		}
	}()
	time.Sleep(20 * time.Millisecond) // let it join the queue before the next one
}

// A new channel (its viewer sees a black screen) gets the slot before
// channels that are already playing, even if those were waiting first.
func TestNewChannelGoesFirst(t *testing.T) {
	s := newProviderSlot()
	ctx := context.Background()
	s.acquire(ctx, false)

	got := make(chan string, 3)
	waitTurn(s, ctx, false, "playing 1", got)
	waitTurn(s, ctx, false, "playing 2", got)
	waitTurn(s, ctx, true, "new", got)

	var order []string
	for i := 0; i < 3; i++ {
		s.release()
		order = append(order, <-got)
	}
	if want := "[new playing 1 playing 2]"; fmt.Sprint(order) != want {
		t.Errorf("turns went %v, want %s", order, want)
	}
}

// A waiter that gives up leaves the queue, and the slot goes to the next one.
func TestGivingUpLeavesTheQueue(t *testing.T) {
	s := newProviderSlot()
	s.acquire(context.Background(), false)

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan string, 2)
	waitTurn(s, ctx, true, "gave up", got)
	waitTurn(s, context.Background(), false, "next", got)
	cancel()
	time.Sleep(20 * time.Millisecond)

	s.release()
	if who := <-got; who != "next" {
		t.Errorf("the slot went to %q", who)
	}
	s.release()
	if !s.acquire(context.Background(), false) {
		t.Error("the slot stayed taken after everyone released it")
	}
}

// waitTurnBy is waitTurn with an urgency function.
func waitTurnBy(s *providerSlot, urgency func() float64, name string, got chan<- string) {
	go func() {
		if s.acquireBy(context.Background(), urgency) {
			got <- name
		}
	}()
	time.Sleep(20 * time.Millisecond)
}

// The slot goes to the channel whose viewers are closest to running out,
// whatever order they started waiting in.
func TestSlotGoesToLeastBuffer(t *testing.T) {
	s := newProviderSlot()
	s.acquire(context.Background(), false)

	got := make(chan string, 3)
	waitTurnBy(s, func() float64 { return 25 }, "25s left", got)
	waitTurnBy(s, func() float64 { return 8 }, "8s left", got)
	waitTurnBy(s, func() float64 { return 15 }, "15s left", got)

	var order []string
	for i := 0; i < 3; i++ {
		s.release()
		order = append(order, <-got)
	}
	if want := "[8s left 15s left 25s left]"; fmt.Sprint(order) != want {
		t.Errorf("turns went %v, want %s", order, want)
	}
}

// Urgency is read when the slot frees up, not when the wait began: a channel
// whose viewers ran low while it waited goes first.
func TestSlotReadsUrgencyWhenFree(t *testing.T) {
	s := newProviderSlot()
	s.acquire(context.Background(), false)

	var left atomic.Int64
	left.Store(30)
	got := make(chan string, 2)
	waitTurnBy(s, func() float64 { return float64(left.Load()) }, "draining", got)
	waitTurnBy(s, func() float64 { return 10 }, "steady", got)
	left.Store(2) // its viewers ran low while it waited

	s.release()
	if who := <-got; who != "draining" {
		t.Errorf("the slot went to %q, want the channel that ran low", who)
	}
	s.release()
	<-got
}

// cushion is how much stream the closest-to-empty viewer has left.
func TestCushionIsLeastLeftAmongViewers(t *testing.T) {
	ch, clk := pacedChannel(t)
	if got := ch.cushion(); got != urgentNow {
		t.Errorf("a channel with nothing yet has cushion %v, want %v", got, urgentNow)
	}

	a := ch.addClient()
	sendSeconds(ch, 0, 20, 2)
	takeReady(t, ch, a) // a plays from 0s, newest is 20s
	clk.advance(5 * time.Second)
	if got := ch.cushion(); !near(got, 15) {
		t.Errorf("cushion %.1fs, want 15s (20s newest - 5s played)", got)
	}

	b := ch.addClient()
	if got := ch.cushion(); got != urgentNow {
		t.Errorf("with a viewer that has not started, cushion %v, want %v", got, urgentNow)
	}
	// b joined with less than JoinBack in the history, so it starts at the
	// newest keyframe with nothing to spare: it is the one closest to running
	// out, and the channel's cushion follows it
	sendSeconds(ch, 21, 21, 0)
	takeReady(t, ch, b) // b starts playing at 21s, the newest
	clk.advance(time.Second)
	if got := ch.cushion(); !near(got, -1) {
		t.Errorf("cushion %.1fs, want -1s (b played past the newest, a still has 15s)", got)
	}
}

// A struggling channel (its replay keeps getting cut) waits while another channel has less
// than StruggleYield left, but still goes before channels with plenty left.
func TestStrugglingChannelYields(t *testing.T) {
	ch, clk := pacedChannel(t)
	a := ch.addClient()
	sendSeconds(ch, 0, 20, 2)
	takeReady(t, ch, a)
	clk.advance(18 * time.Second) // 2s left

	ch.struggling.Store(true)
	got := ch.cushion()
	if !near(got, StruggleYield+2) {
		t.Errorf("struggling cushion %.1f, want %.1f", got, StruggleYield+2)
	}
	if got <= StruggleYield-1 {
		t.Errorf("struggling channel (%.1f) would go before a channel with %.0fs left", got, StruggleYield-1)
	}
	if got >= 20 {
		t.Errorf("struggling channel (%.1f) would go after a channel with 20s left", got)
	}

	ch.struggling.Store(false)
	if got := ch.cushion(); !near(got, 2) {
		t.Errorf("after it recovers cushion %.1f, want 2", got)
	}
}

// A channel cut off in the middle of its replay holds the slot twice as long
// on its next turn, up to SlotHoldMax; once it gets through, back to SlotHold.
func TestSlotHoldGrowsAfterFailures(t *testing.T) {
	oldHold, oldMax := SlotHold, SlotHoldMax
	SlotHold, SlotHoldMax = 2*time.Second, 6*time.Second
	t.Cleanup(func() { SlotHold, SlotHoldMax = oldHold, oldMax })

	ch := newChannel("k", nil, nil)
	for streak, want := range []time.Duration{2 * time.Second, 4 * time.Second, 6 * time.Second, 6 * time.Second, 6 * time.Second} {
		ch.failStreak = streak
		if got := ch.slotHold(); got != want {
			t.Errorf("after %d failures in a row: hold %v, want %v", streak, got, want)
		}
	}
}

// The failure count goes up when the replay is cut short and back to zero
// when the next connection gets through.
func TestFailStreakCountsCutReplays(t *testing.T) {
	data := make([]byte, 1024*1024)
	rand.New(rand.NewSource(4)).Read(data)

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)
	h.mu.RLock()
	ch := h.channels["live/1.ts"]
	h.mu.RUnlock()

	streak := make(chan int, 1)
	go func() {
		w := <-src.w
		writeAll(w, data[:600*1024])
		w.Close()
		w = <-src.w
		writeAll(w, data[100*1024:400*1024]) // cut short in the replay
		w.Close()
		w = <-src.w // third connection: readLoop for the second one has finished
		streak <- ch.failStreak
		writeAll(w, data[200*1024:])
	}()

	if got := readAll(t, c, len(data)); !bytes.Equal(got, data) {
		t.Fatal("viewer got the wrong stream")
	}
	if n := <-streak; n != 1 {
		t.Errorf("after a cut replay the streak is %d, want 1", n)
	}
}

// acquire is acquireBy with a fixed urgency: urgent=true goes before everyone, the rest in order.
func (s *providerSlot) acquire(ctx context.Context, urgent bool) bool {
	u := urgentLater
	if urgent {
		u = urgentNow
	}
	return s.acquireBy(ctx, func() float64 { return u })
}
