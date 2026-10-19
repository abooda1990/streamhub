package streamhub

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"testing"
)

// pcrPacket builds one TS packet whose adaptation field carries a PCR of the
// given time in seconds (ext part left at 0).
func pcrPacket(pid int, seconds float64, keyframe bool) []byte {
	p := bytes.Repeat([]byte{0xff}, tsPacketSize)
	p[0] = tsSyncByte
	p[1] = byte(pid>>8) & 0x1f
	p[2] = byte(pid)
	p[3] = 0x30 // adaptation field + payload
	p[4] = 7    // flags + 6 PCR bytes
	p[5] = 0x10 // PCR_flag
	if keyframe {
		p[5] |= 0x40 // random_access_indicator
	}
	base := uint64(math.Round(seconds*90_000)) % (1 << 33)
	p[6] = byte(base >> 25)
	p[7] = byte(base >> 17)
	p[8] = byte(base >> 9)
	p[9] = byte(base >> 1)
	p[10] = byte(base&1)<<7 | 0x7e // 6 reserved bits set, ext high bit 0
	p[11] = 0
	return p
}

func near(a, b float64) bool { return math.Abs(a-b) < 0.001 }

func TestReadPCR(t *testing.T) {
	got, ok := readPCR(pcrPacket(256, 30, false))
	if !ok || !near(got, 30) {
		t.Fatalf("readPCR = %v, %v; want 30s", got, ok)
	}
	if _, ok := readPCR(tsPacket(256, false, false, 'v')); ok {
		t.Error("a packet without a PCR gave one")
	}
}

// Reads from the provider split packets anywhere; the clock must still read
// every PCR, and time counts from the first one.
func TestClockReadsPacketsSplitAcrossChunks(t *testing.T) {
	var ts []byte
	ts = append(ts, 0x12, 0x34) // stray bytes before the first packet
	for i := 0; i <= 50; i++ {
		ts = append(ts, pcrPacket(256, 100+float64(i)*0.1, false)...)
		ts = append(ts, tsPacket(256, false, false, 'v')...)
	}

	var c tsClock
	var at float64
	for b := ts; len(b) > 0; {
		n := min(1000, len(b))
		at, _ = c.feed(b[:n])
		b = b[n:]
	}
	if !near(at, 5) {
		t.Errorf("clock at %.3fs, want 5s (50 steps of 0.1s)", at)
	}
}

// A PCR that goes back, or jumps far ahead, is a new start of the stream, not
// time that passed: the clock carries on from where it was.
func TestClockIgnoresJumps(t *testing.T) {
	var c tsClock
	feed := func(sec float64) float64 {
		at, _ := c.feed(pcrPacket(256, sec, false))
		return at
	}
	feed(10)
	feed(11)
	feed(12)
	if at := feed(5000); !near(at, 2) {
		t.Errorf("after a jump ahead the clock is at %.3fs, want 2s", at)
	}
	if at := feed(5001); !near(at, 3) {
		t.Errorf("after the jump the clock is at %.3fs, want 3s", at)
	}
	if at := feed(4000); !near(at, 3) {
		t.Errorf("after a jump back the clock is at %.3fs, want 3s", at)
	}
}

// The PCR wraps to zero after ~26.5 hours; that is still one step forward.
func TestClockAcrossPCRWrap(t *testing.T) {
	var c tsClock
	c.feed(pcrPacket(256, pcrWrap-0.5, false))
	if at, _ := c.feed(pcrPacket(256, 0.5, false)); !near(at, 1) {
		t.Errorf("across the wrap the clock is at %.3fs, want 1s", at)
	}
}

// Only the first PID that carries a PCR drives the clock.
func TestClockKeepsToOnePCRPID(t *testing.T) {
	var c tsClock
	c.feed(pcrPacket(256, 10, false))
	c.feed(pcrPacket(300, 50, false)) // another program's clock
	if at, _ := c.feed(pcrPacket(256, 11, false)); !near(at, 1) {
		t.Errorf("clock at %.3fs, want 1s", at)
	}
}

func TestClockMarksKeyframes(t *testing.T) {
	var c tsClock
	if _, key := c.feed(pcrPacket(256, 1, false)); key {
		t.Error("a packet without random access was marked as a keyframe")
	}
	if _, key := c.feed(pcrPacket(256, 2, true)); !key {
		t.Error("a keyframe packet was not marked")
	}
}

// The history keeps BufferSeconds of stream when the stream has a PCR.
func TestHistoryKeepsBufferSeconds(t *testing.T) {
	ch := newChannel("k", nil, nil)
	for i := 0; i <= 100; i++ { // one chunk per second of stream
		chunk := append(pcrPacket(256, float64(i), false), tsPacket(256, false, false, 'v')...)
		ch.broadcast(chunk)
	}
	if got := ch.bufferedSeconds(); got < BufferSeconds-1 || got > BufferSeconds {
		t.Errorf("history holds %.1fs, want %.0fs", got, BufferSeconds)
	}
}

// tsStream builds a TS stream from `from` to `to` seconds: every 0.1s one
// packet with a PCR and one with random payload (so any 32KB is unique).
func tsStream(from, to float64, rng *rand.Rand) []byte {
	var b []byte
	for s := from; s <= to+1e-9; s += 0.1 {
		b = append(b, pcrPacket(256, s, false)...)
		p := make([]byte, tsPacketSize)
		rng.Read(p)
		p[0], p[1], p[2], p[3] = tsSyncByte, 0x01, 0x00, 0x10 // PID 256, payload only
		b = append(b, p...)
	}
	return b
}

// After a gap longer than the provider's replay, the last thing we sent is no
// longer in the replay. The new data starts after it, so it all goes out,
// instead of searching for it forever and dropping every connection.
func TestReplayWithoutOverlapIsSent(t *testing.T) {
	old := Pacing
	Pacing = false
	t.Cleanup(func() { Pacing = old })

	rng := rand.New(rand.NewSource(7))
	first := tsStream(0, 20, rng)
	second := tsStream(45, 50, rng) // the provider only kept 45s..50s

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)
	go func() {
		w := <-src.w
		writeAll(w, first)
		w.Close()
		w = <-src.w
		writeAll(w, second)
		w.Close()
	}()

	want := append(append([]byte(nil), first...), second...)
	if got := readAll(t, c, len(want)); !bytes.Equal(got, want) {
		t.Fatalf("viewer got %d bytes, want the first connection then all of the second", len(got))
	}
}

// With a PCR and a real overlap, the repeated part is still dropped.
func TestReplayWithOverlapIsStillDeduped(t *testing.T) {
	old := Pacing
	Pacing = false
	t.Cleanup(func() { Pacing = old })

	rng := rand.New(rand.NewSource(8))
	all := tsStream(0, 30, rng)
	sent := len(all) * 2 / 3 // the first connection gets this far
	replay := len(all) / 3   // the second one starts back here

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)
	go func() {
		w := <-src.w
		writeAll(w, all[:sent])
		w.Close()
		writeAll(<-src.w, all[replay:])
	}()

	if got := readAll(t, c, len(all)); !bytes.Equal(got, all) {
		t.Fatalf("viewer got %d bytes that differ from the stream sent once", len(got))
	}
}

// A PCR step of a few seconds is not time that passed (a PCR comes every
// 100ms at most): the clock does not jump with it.
func TestClockIgnoresSmallJumps(t *testing.T) {
	var c tsClock
	c.feed(pcrPacket(256, 0, false))
	c.feed(pcrPacket(256, 0.1, false))
	if at, _ := c.feed(pcrPacket(256, 5.1, false)); !near(at, 0.1) {
		t.Errorf("after a 5s jump the clock is at %.3fs, want 0.1s", at)
	}
}

// After a reconnect the clock may jump by hours (some channels do it on the
// first reconnect). That is not a real gap: we look for the repeat by bytes,
// and if that keeps failing, the safety net sends the replay as it is.
func TestClockJumpAtReconnectFallsBackToSafetyNet(t *testing.T) {
	old := Pacing
	Pacing = false
	t.Cleanup(func() { Pacing = old })

	rng := rand.New(rand.NewSource(9))
	first := tsStream(0, 20, rng)
	jumped := tsStream(30000, 30005, rng) // hours "later": not believed
	last := tsStream(30005.1, 30010, rng)

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)
	go func() {
		w := <-src.w
		writeAll(w, first)
		w.Close()
		for i := 0; i < dedupeBlindFails; i++ { // searched by bytes, not found, cut
			w = <-src.w
			writeAll(w, jumped)
			w.Close()
		}
		writeAll(<-src.w, last) // the safety net sends this one as it is
	}()

	want := append(append([]byte(nil), first...), last...)
	if got := readAll(t, c, len(want)); !bytes.Equal(got, want) {
		t.Fatalf("viewer got %d bytes, want the first connection then the last one", len(got))
	}
}
