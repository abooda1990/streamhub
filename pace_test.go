package streamhub

import (
	"testing"
	"time"
)

// fakeClock replaces the channel's clock, so pacing can be tested without
// waiting for real seconds.
type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func pacedChannel(t *testing.T) (*Channel, *fakeClock) {
	t.Helper()
	withPreRoll(t, 0)
	old := Pacing
	Pacing = true
	t.Cleanup(func() { Pacing = old })
	clk := &fakeClock{t: time.Unix(1000, 0)}
	ch := newChannel("k", nil, nil)
	ch.now = clk.now
	return ch, clk
}

// sendSeconds broadcasts one chunk per second of stream, from..to, with a
// keyframe every `every` seconds (0: none).
func sendSeconds(ch *Channel, from, to, every int) {
	for s := from; s <= to; s++ {
		key := every > 0 && s%every == 0
		chunk := append(pcrPacket(256, float64(s), key), tsPacket(256, false, false, 'v')...)
		ch.broadcast(chunk)
	}
}

// takeReady returns the stream times of the chunks the viewer may have right
// now, and when the next one is due.
func takeReady(t *testing.T, ch *Channel, c *Client) (got []float64, until time.Time) {
	t.Helper()
	for {
		chunk, _, u, err := ch.nextFor(c)
		if err != nil {
			t.Fatal(err)
		}
		if chunk == nil {
			return got, u
		}
		pcr, _ := readPCR(chunk)
		got = append(got, pcr)
	}
}

// The first PaceLead of stream goes out at once (it fills the player), the
// rest at playing speed.
func TestPacingSendsLeadThenRealTime(t *testing.T) {
	ch, clk := pacedChannel(t)
	c := ch.addClient()
	sendSeconds(ch, 0, 30, 0)

	got, until := takeReady(t, ch, c)
	if want := int(PaceLead.Seconds()) + 1; len(got) != want { // 0..4
		t.Fatalf("got %d chunks at once (%v), want %d", len(got), got, want)
	}
	if want := clk.t.Add(time.Second); !until.Equal(want) {
		t.Errorf("next chunk due at %v, want one second later", until.Sub(clk.t))
	}

	// the player reads all the time: one more second of stream each second
	got = nil
	for i := 0; i < 10; i++ {
		clk.advance(time.Second)
		more, _ := takeReady(t, ch, c)
		got = append(got, more...)
	}
	if len(got) != 10 || got[0] != 5 || got[9] != 14 {
		t.Errorf("over 10s got %v, want the chunks for seconds 5..14", got)
	}
}

// A viewer joining a running channel starts at a keyframe JoinBack seconds
// back, so it has the same protection from gaps as everyone else.
func TestPacedJoinerStartsJoinBackBehind(t *testing.T) {
	ch, _ := pacedChannel(t)
	sendSeconds(ch, 0, 39, 2) // keyframes at even seconds
	c := ch.addClient()
	defer c.Close()

	got, _ := takeReady(t, ch, c)
	// newest is 39s: 39-30 = 9, the last keyframe at or before it is 8s, and
	// the chunk before it (7s) is included for a packet cut across chunks
	if len(got) == 0 || got[0] != 7 {
		t.Errorf("joiner starts at %v, want 7s", got)
	}
}

// When the player ran out of data (a gap longer than the buffer), the clock
// restarts where it stopped: the rest is paced again, not sent all at once.
func TestPacingRestartsAfterStall(t *testing.T) {
	ch, clk := pacedChannel(t)
	c := ch.addClient()
	sendSeconds(ch, 0, 4, 0)
	takeReady(t, ch, c) // 0..4 at once

	clk.advance(20 * time.Second) // nothing arrived: the player stopped at 4s
	sendSeconds(ch, 5, 30, 0)
	got, _ := takeReady(t, ch, c)
	// the player resumes at 4s and gets one lead ahead of it: 5..8
	if len(got) != int(PaceLead.Seconds()) || got[0] != 5 {
		t.Errorf("after the stall got %v, want 5..8 (one lead), not everything", got)
	}
}

// A stream without a PCR is not paced: every chunk goes out at once, as before.
func TestNoPacingWithoutPCR(t *testing.T) {
	ch, _ := pacedChannel(t)
	c := ch.addClient()
	for i := 0; i < 50; i++ {
		ch.broadcast([]byte{byte(i)})
	}
	for i := 0; i < 50; i++ {
		chunk, _, until, _ := ch.nextFor(c)
		if chunk == nil {
			t.Fatalf("chunk %d held back (due %v) on a stream without a PCR", i, until)
		}
	}
}

// Recv waits for a paced chunk's time and then returns it.
func TestRecvWaitsForPacedChunk(t *testing.T) {
	withPreRoll(t, 0)
	old, oldLead := Pacing, PaceLead
	Pacing, PaceLead = true, 0
	t.Cleanup(func() { Pacing, PaceLead = old, oldLead })

	ch := newChannel("k", nil, nil)
	c := ch.addClient()
	defer c.Close()
	ch.broadcast(append(pcrPacket(256, 0, false), tsPacket(256, false, false, 'v')...))
	ch.broadcast(append(pcrPacket(256, 0.1, false), tsPacket(256, false, false, 'v')...))

	recv(t, c) // 0s goes out at once
	start := time.Now()
	recv(t, c) // 0.1s is due 100ms later
	if d := time.Since(start); d < 80*time.Millisecond {
		t.Errorf("paced chunk came after %v, want about 100ms", d)
	}
}

// putPieces adds pieces with the given stream times straight to the history
// (the clock would already smooth out a jump; this tests the pacing on its own).
func putPieces(ch *Channel, times ...float64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, at := range times {
		ch.history = append(ch.history, piece{data: []byte{byte(at)}, at: at})
	}
	ch.hasPCR = true
}

// When the stream's own time jumps ahead, the viewer does not wait through
// the jump (it used to wait, think the player had stopped, set its clock back
// and wait again, forever).
func TestPacingSkipsAJumpInStreamTime(t *testing.T) {
	ch, clk := pacedChannel(t)
	c := ch.addClient()
	putPieces(ch, 0, 1, 2, 3, 4, 10, 11)

	got := 0
	for i := 0; i < 5; i++ { // the player reads all the time
		for {
			chunk, _, _, _ := ch.nextFor(c)
			if chunk == nil {
				break
			}
			got++
		}
		clk.advance(time.Second)
	}
	if got != 7 {
		t.Errorf("after 5s the viewer had %d of 7 chunks: it is stuck at the jump", got)
	}
}
