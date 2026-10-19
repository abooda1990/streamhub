package streamhub

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func withOpenTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := OpenTimeout
	OpenTimeout = d
	t.Cleanup(func() { OpenTimeout = old })
}

// A provider that never answers: the attempt gives up after OpenTimeout and frees the
// connection lock, instead of holding it for the whole HTTP timeout.
func TestOpenGivesUpWhenProviderDoesNotAnswer(t *testing.T) {
	withOpenTimeout(t, 100*time.Millisecond)
	h := NewHub()
	silent := func(ctx context.Context) (io.ReadCloser, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	start := time.Now()
	if _, err := h.Subscribe(context.Background(), "live/dead.ts", silent); err == nil {
		t.Fatal("subscribing to a silent provider succeeded")
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("gave up after %v, want about OpenTimeout", d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !h.slot.acquire(ctx, false) {
		t.Fatal("the connection lock was not freed after the failed open")
	}
	h.slot.release()
}

// The timeout is only for the answer: once connected, the stream keeps going past it.
func TestStreamOutlivesOpenTimeout(t *testing.T) {
	withOpenTimeout(t, 50*time.Millisecond)
	withPreRoll(t, 0)
	h := NewHub()
	w := make(chan *io.PipeWriter, 1)
	open := func(ctx context.Context) (io.ReadCloser, error) {
		r, pw := io.Pipe()
		go func() { <-ctx.Done(); pw.CloseWithError(ctx.Err()) }() // like an HTTP body tied to ctx
		w <- pw
		return r, nil
	}
	c, err := h.Subscribe(context.Background(), "live/1.ts", open)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Unsubscribe(c)
	pw := <-w
	time.Sleep(200 * time.Millisecond) // well past OpenTimeout
	go pw.Write([]byte("still here"))
	if got := recv(t, c); got != "still here" {
		t.Errorf("got %q after OpenTimeout, want the stream to keep going", got)
	}
}

// A channel the provider refused goes to the back of the queue, and so does the same
// channel opened again soon after.
func TestRefusedChannelWaitsAtTheBack(t *testing.T) {
	h := NewHub()
	refuse := func(ctx context.Context) (io.ReadCloser, error) {
		return nil, errors.New("upstream returned 407")
	}
	if _, err := h.Subscribe(context.Background(), "live/event.ts", refuse); err == nil {
		t.Fatal("subscribing to a refusing provider succeeded")
	}

	h.mu.Lock()
	dead := h.recentlyDead("live/event.ts")
	h.mu.Unlock()
	if !dead {
		t.Fatal("the hub does not remember the failed channel")
	}

	ch := newChannel("live/event.ts", refuse, nil)
	ch.openFailed.Store(true)
	ch.addClient()
	if got := ch.cushion(); got < urgentLater {
		t.Errorf("refused channel has urgency %v, want the back of the queue", got)
	}
}
