package streamhub

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Hub manages all the open channels
type Hub struct {
	mu       sync.RWMutex
	channels map[string]*Channel

	// slot is the provider connection lock, shared by all channels: the provider account has one
	// connection, so only one channel connects at a time, and holds it until its replay is done.
	slot *providerSlot

	// deadUntil: channels whose first connection failed, and until when a new attempt starts
	// at the back of the queue (DeadCooldown). Guarded by mu.
	deadUntil map[string]time.Time
}

func NewHub() *Hub {
	return &Hub{
		channels:  make(map[string]*Channel),
		slot:      newProviderSlot(),
		deadUntil: make(map[string]time.Time),
	}
}

// markDead remembers that a channel could not be opened, for DeadCooldown
func (h *Hub) markDead(key string) {
	h.mu.Lock()
	h.deadUntil[key] = time.Now().Add(DeadCooldown)
	h.mu.Unlock()
}

// recentlyDead is true if the channel failed to open less than DeadCooldown ago. The caller holds h.mu.
func (h *Hub) recentlyDead(key string) bool {
	until, ok := h.deadUntil[key]
	if ok && time.Now().After(until) {
		delete(h.deadUntil, key)
		return false
	}
	return ok
}

// Subscribe joins a channel, or opens it if it does not exist.
// Opening the upstream happens outside the lock, so a slow channel does not hold up the others,
// and several clients asking for the same channel at once wait for the same connection.
func (h *Hub) Subscribe(ctx context.Context, key string, open OpenFunc) (*Client, error) {
	h.mu.Lock()
	ch := h.channels[key]
	var client *Client
	if ch != nil {
		client = ch.addClient()
	}
	// close the abandoned channels, whether a new channel opens or someone joins an open one
	idle := h.takeIdle(key)
	isNew := client == nil
	if isNew {
		// no channel, or it is still closing: open a new one
		ch = newChannel(key, open, h.remove)
		ch.slot = h.slot
		ch.onOpenFail = h.markDead
		if h.recentlyDead(key) {
			// it failed to open a moment ago (e.g. an event channel with no event): try it
			// again only when nobody else needs the connection
			ch.openFailed.Store(true)
			log.Printf("[hub] %s failed to open less than %s ago: it waits at the back of the queue", key, DeadCooldown)
		}
		h.channels[key] = ch
		client = ch.addClient()
	}
	h.mu.Unlock()

	// close the channels nobody watches before opening the new one: the provider account
	// is limited, and they used to cut off the watched channel for 30 seconds.
	for _, c := range idle {
		log.Printf("[hub] closing idle channel %s (viewer went to %s)", c.key, key)
		c.close()
	}
	if isNew {
		go ch.run()
	}

	select {
	case <-ch.ready:
	case <-ctx.Done():
		ch.removeClient(client)
		return nil, ctx.Err()
	}
	if ch.startErr != nil {
		ch.removeClient(client)
		return nil, fmt.Errorf("open upstream: %w", ch.startErr)
	}
	return client, nil
}

// AcquireSlot waits for a turn on the provider connection for something that is not a live channel
// (e.g. an on-demand file download). urgency is compared with the live channels' cushions (seconds left), so
// a live channel with less than that left goes first. release frees the lock, and is safe to call
// more than once. ok=false if ctx is done before the turn comes.
func (h *Hub) AcquireSlot(ctx context.Context, urgency float64) (release func(), ok bool) {
	if !h.slot.acquireBy(ctx, func() float64 { return urgency }) {
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(h.slot.release) }, true
}

// takeIdle removes from the map every channel nobody watches (except keep,
// the channel asked for now) and returns them. The caller holds h.mu and must close them after releasing the lock.
func (h *Hub) takeIdle(keep string) []*Channel {
	var idle []*Channel
	for k, ch := range h.channels {
		if k != keep && ch.viewerCount() == 0 {
			delete(h.channels, k)
			idle = append(idle, ch)
		}
	}
	return idle
}

// remove takes the channel out of the map, but only if it is still the same one (not a new one with the same key)
func (h *Hub) remove(ch *Channel) {
	h.mu.Lock()
	if h.channels[ch.key] == ch {
		delete(h.channels, ch.key)
	}
	h.mu.Unlock()
}

// Unsubscribe removes a client from its channel
func (h *Hub) Unsubscribe(client *Client) {
	client.owner.removeClient(client)
}

// Stats returns the stats of every channel
func (h *Hub) Stats() map[string]ChannelStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make(map[string]ChannelStats, len(h.channels))
	for k, ch := range h.channels {
		out[k] = ch.Stats()
	}
	return out
}
