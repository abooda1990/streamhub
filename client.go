package streamhub

import (
	"context"
	"io"
	"sync"
	"time"
)

type Client struct {
	closed chan struct{}
	once   sync.Once
	id     string
	owner  *Channel

	// Instead of a queue per viewer: a cursor into the channel's history. Each viewer reads
	// at its own pace, so a slow one holds nobody back. Both fields are guarded by owner.mu.
	next uint64   // number of the next piece this viewer must get (see Channel.base)
	head [][]byte // data to give before the cursor (from the keyframe or the pre-roll)

	// Playback clock (for pacing): the player is showing about stream time
	// playAt + (now - playWall). Guarded by owner.mu.
	anchored bool
	playAt   float64
	playWall time.Time
	lastAt   float64 // stream time of the last piece we sent
	sent     bool    // have we sent anything since the clock was set?
}

func newClient(id string, owner *Channel) *Client {
	return &Client{
		closed: make(chan struct{}),
		id:     id,
		owner:  owner,
	}
}

// Recv returns the viewer's next piece from the history, and if it has not arrived yet it waits
// until it does, or the viewer closes, or ctx is done.
// (with pacing: if the piece is there but it is still too early for it, nextFor returns until,
// so we also wait for that time)
func (c *Client) Recv(ctx context.Context) ([]byte, error) {
	for {
		if c.isClosed() {
			return nil, io.EOF
		}
		chunk, wait, until, err := c.owner.nextFor(c)
		if chunk != nil || err != nil {
			return chunk, err
		}
		var due <-chan time.Time
		var timer *time.Timer
		if !until.IsZero() {
			timer = time.NewTimer(time.Until(until))
			due = timer.C
		}
		select {
		case <-wait: // a new piece arrived
		case <-due: // the piece's time has come
		case <-c.closed:
			stopTimer(timer)
			return nil, io.EOF
		case <-ctx.Done():
			stopTimer(timer)
			return nil, ctx.Err()
		}
		stopTimer(timer)
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.closed)
	})
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
