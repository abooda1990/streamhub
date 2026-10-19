package streamhub

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// pipeSource opens an upstream the test writes to, and counts the opens.
type pipeSource struct {
	opens atomic.Int32
	w     chan *io.PipeWriter
}

func newPipeSource() *pipeSource {
	return &pipeSource{w: make(chan *io.PipeWriter, 16)}
}

func (p *pipeSource) open(ctx context.Context) (io.ReadCloser, error) {
	p.opens.Add(1)
	r, w := io.Pipe()
	p.w <- w
	return r, nil
}

func recv(t *testing.T, c *Client) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	chunk, err := c.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	return string(chunk)
}

func TestViewersOfOneChannelShareOneUpstream(t *testing.T) {
	src := newPipeSource()
	h := NewHub()
	ctx := context.Background()

	a, err := h.Subscribe(ctx, "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.Subscribe(ctx, "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	if n := src.opens.Load(); n != 1 {
		t.Fatalf("opened the upstream %d times, want 1", n)
	}

	w := <-src.w
	go w.Write([]byte("x"))
	if got := recv(t, a); got != "x" {
		t.Errorf("a got %q", got)
	}
	if got := recv(t, b); got != "x" {
		t.Errorf("b got %q", got)
	}
	h.Unsubscribe(a)
	h.Unsubscribe(b)
}

// A channel that failed to open must not stay in the hub, or every later
// viewer would hang on it until the server restarts.
func TestFailedChannelIsRemoved(t *testing.T) {
	h := NewHub()
	fail := func(ctx context.Context) (io.ReadCloser, error) { return nil, errors.New("down") }

	if _, err := h.Subscribe(context.Background(), "live/1.ts", fail); err == nil {
		t.Fatal("want an error when the upstream is down")
	}
	deadline := time.Now().Add(time.Second)
	for len(h.Stats()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the failed channel stayed in the hub")
		}
		time.Sleep(10 * time.Millisecond)
	}

	src := newPipeSource()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatalf("a later viewer could not open the channel: %v", err)
	}
	h.Unsubscribe(c)
}

// The upstream ending makes the channel reconnect at once, and viewers keep
// receiving without being cut off.
func TestChannelReconnectsWhenUpstreamEnds(t *testing.T) {
	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)

	w := <-src.w
	go func() { w.Write([]byte("a")); w.Close() }()
	if got := recv(t, c); got != "a" {
		t.Fatalf("got %q", got)
	}

	var w2 *io.PipeWriter
	select {
	case w2 = <-src.w:
	case <-time.After(time.Second):
		t.Fatal("the channel did not reconnect right away")
	}
	go w2.Write([]byte("b"))
	if got := recv(t, c); got != "b" {
		t.Errorf("after reconnecting got %q", got)
	}
}

// Each viewer reads at its own pace from the shared history: one that reads
// nothing does not hold back another one.
func TestSlowViewerDoesNotHoldBackOthers(t *testing.T) {
	withPreRoll(t, 0)
	ch := newChannel("k", nil, nil)
	slow, fast := ch.addClient(), ch.addClient()
	defer slow.Close()
	defer fast.Close()

	for i := 0; i < 100; i++ {
		ch.broadcast([]byte{byte(i)})
		if got := recv(t, fast); got != string([]byte{byte(i)}) {
			t.Fatalf("fast viewer got %q at chunk %d", got, i)
		}
	}
	if got := recv(t, slow); got != string([]byte{0}) {
		t.Errorf("slow viewer starts with %q, want the first chunk", got)
	}
}

// A viewer that falls further behind than the history holds skips ahead to
// the oldest data left, instead of getting nothing.
func TestViewerTooFarBehindSkipsAhead(t *testing.T) {
	withPreRoll(t, 0)
	ch := newChannel("k", nil, nil)
	c := ch.addClient()
	defer c.Close()

	chunk := make([]byte, 64*1024)
	for i := 0; i < 2*HistoryBytes/len(chunk); i++ { // twice what the history keeps
		chunk = append([]byte(nil), chunk...)
		chunk[0] = byte(i)
		ch.broadcast(chunk)
	}
	got, err := c.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	oldest := ch.history[0].data
	ch.mu.Unlock()
	if !bytes.Equal(got, oldest) {
		t.Errorf("viewer resumed at chunk %d, want the oldest one left (%d)", got[0], oldest[0])
	}
}

// Recv waits for the next chunk and wakes up as soon as it arrives.
func TestRecvWaitsForNewData(t *testing.T) {
	withPreRoll(t, 0)
	ch := newChannel("k", nil, nil)
	c := ch.addClient()
	defer c.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		ch.broadcast([]byte("late"))
	}()
	if got := recv(t, c); got != "late" {
		t.Errorf("got %q, want the chunk sent after Recv started waiting", got)
	}
}

// A new viewer starts from the newest buffered data, not the oldest.
func TestNewViewerGetsNewestPreRoll(t *testing.T) {
	withPreRoll(t, 1<<20)
	ch := newChannel("k", nil, nil)
	for i := 0; i < ClientBufSize; i++ {
		ch.broadcast([]byte{byte(i % 256)})
	}
	ch.broadcast([]byte("last"))

	c := ch.addClient()
	if got, want := len(c.head), ClientBufSize/2; got != want {
		t.Fatalf("pre-roll gave %d chunks, want %d", got, want)
	}
	var last []byte
	for _, b := range c.head {
		last = b
	}
	if string(last) != "last" {
		t.Errorf("pre-roll ended with %q, want the newest chunk", last)
	}
}

// withPreRoll sets the pre-roll for one test (0 turns it off).
func withPreRoll(t *testing.T, n int) {
	old := PreRollBytes
	PreRollBytes = n
	t.Cleanup(func() { PreRollBytes = old })
}

// With the pre-roll off, a new viewer starts at the live edge, with nothing from before.
func TestNoPreRollWhenOff(t *testing.T) {
	withPreRoll(t, 0)
	ch := newChannel("k", nil, nil)
	ch.broadcast([]byte("old"))
	if c := ch.addClient(); len(c.head) != 0 {
		t.Errorf("a new viewer got %d old chunks", len(c.head))
	}
}

// writeAll writes b to w in pieces, the way a provider sends a stream.
func writeAll(w *io.PipeWriter, b []byte) {
	for len(b) > 0 {
		n := min(32*1024, len(b))
		w.Write(b[:n])
		b = b[n:]
	}
}

// readAll collects n bytes from the viewer.
func readAll(t *testing.T, c *Client, n int) []byte {
	t.Helper()
	got := make([]byte, 0, n)
	for len(got) < n {
		got = append(got, recv(t, c)...)
	}
	return got
}

// After a reconnect the provider starts again a few seconds back; the part
// viewers already got must be dropped, or they see the stream repeat.
func TestReconnectDropsRepeatedData(t *testing.T) {
	data := make([]byte, 300*1024)
	rand.New(rand.NewSource(1)).Read(data)

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)

	w := <-src.w
	go func() { writeAll(w, data[:200*1024]); w.Close() }()
	go func() { writeAll(<-src.w, data[120*1024:]) }() // repeats 80 KiB

	if got := readAll(t, c, len(data)); !bytes.Equal(got, data) {
		t.Fatalf("viewer got %d bytes that differ from the %d sent once", len(got), len(data))
	}
}

// The provider may go back further than the history kept here (a high-bitrate channel
// replays 25-30 MB); the repeat must still be dropped.
func TestReconnectDropsReplayLongerThanHistory(t *testing.T) {
	data := make([]byte, HistoryBytes*2+512*1024)
	rand.New(rand.NewSource(2)).Read(data)
	sent := HistoryBytes*2 - 100*1024 // what the first connection gets out

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)

	w := <-src.w
	go func() { writeAll(w, data[:sent]); w.Close() }()
	go func() { writeAll(<-src.w, data) }() // replays everything from the start

	if got := readAll(t, c, len(data)); !bytes.Equal(got, data) {
		t.Fatalf("viewer got %d bytes that differ from the %d sent once", len(got), len(data))
	}
}

// The provider can cut the connection while it is still replaying old data.
// That old data must not go out: viewers would jump back and their player
// would choke on the burst.
func TestReplayCutShortIsDiscarded(t *testing.T) {
	data := make([]byte, 1024*1024)
	rand.New(rand.NewSource(3)).Read(data)

	src := newPipeSource()
	h := NewHub()
	c, err := h.Subscribe(context.Background(), "live/1.ts", src.open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)

	go func() {
		w := <-src.w
		writeAll(w, data[:600*1024])
		w.Close()
		w = <-src.w
		writeAll(w, data[100*1024:400*1024]) // cut short
		w.Close()
		writeAll(<-src.w, data[200*1024:])
	}()

	if got := readAll(t, c, len(data)); !bytes.Equal(got, data) {
		t.Fatalf("viewer got %d bytes that differ from the %d sent once", len(got), len(data))
	}
}

// Opening a channel closes the ones nobody watches any more (they would keep
// a provider connection for the idle timeout), and leaves watched ones alone.
func TestOpeningChannelClosesIdleOnes(t *testing.T) {
	h := NewHub()
	ctx := context.Background()

	left, _ := h.Subscribe(ctx, "live/left.ts", newPipeSource().open)
	watched, _ := h.Subscribe(ctx, "live/watched.ts", newPipeSource().open)
	defer h.Unsubscribe(watched)
	h.Unsubscribe(left)

	c, err := h.Subscribe(ctx, "live/new.ts", newPipeSource().open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)

	stats := h.Stats()
	if _, ok := stats["live/left.ts"]; ok {
		t.Error("the channel nobody watches stayed open")
	}
	if _, ok := stats["live/watched.ts"]; !ok {
		t.Error("a watched channel was closed")
	}
	if watched.isClosed() {
		t.Error("the viewer of the watched channel was cut off")
	}
}

// Joining a channel that is already open also closes the ones nobody watches.
func TestJoiningOpenChannelClosesIdleOnes(t *testing.T) {
	h := NewHub()
	ctx := context.Background()
	src := newPipeSource()

	first, _ := h.Subscribe(ctx, "live/watched.ts", src.open)
	defer h.Unsubscribe(first)
	left, _ := h.Subscribe(ctx, "live/left.ts", newPipeSource().open)
	h.Unsubscribe(left)

	second, err := h.Subscribe(ctx, "live/watched.ts", src.open) // joins, opens nothing
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(second)

	if _, ok := h.Stats()["live/left.ts"]; ok {
		t.Error("the channel nobody watches stayed open")
	}
	if n := src.opens.Load(); n != 1 {
		t.Errorf("joining reopened the watched channel (%d opens)", n)
	}
}

// An empty channel stays open for the idle timeout, so a player that drops and
// reconnects within a second or two finds the same channel, then it closes.
func TestEmptyChannelClosesAfterIdleTimeout(t *testing.T) {
	old := IdleTimeout
	IdleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { IdleTimeout = old })

	h := NewHub()
	ctx := context.Background()
	watched, _ := h.Subscribe(ctx, "live/watched.ts", newPipeSource().open)
	defer h.Unsubscribe(watched)
	left, _ := h.Subscribe(ctx, "live/left.ts", newPipeSource().open)
	h.Unsubscribe(left)

	if _, ok := h.Stats()["live/left.ts"]; !ok {
		t.Fatal("the empty channel closed before the idle timeout")
	}
	time.Sleep(10 * IdleTimeout)
	if _, ok := h.Stats()["live/left.ts"]; ok {
		t.Error("the empty channel stayed open after the idle timeout")
	}
	if watched.isClosed() {
		t.Error("the watched channel was closed")
	}
}
