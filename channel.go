package streamhub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// OpenFunc opens the upstream (the first time and on every reconnect)
type OpenFunc func(ctx context.Context) (io.ReadCloser, error)

var clientSeq atomic.Uint64

type Channel struct {
	key     string
	open    OpenFunc
	onClose func(*Channel)
	slot    *providerSlot

	ready    chan struct{}
	startErr error

	mu       sync.Mutex
	upstream io.ReadCloser
	clients  map[*Client]struct{}
	closed   bool

	// history     [][]byte
	//
	// The last BufferSeconds of stream, each piece with its stream time (from the PCR). If the stream
	// has no PCR, it falls back to the old limit in bytes (HistoryBytes).
	history     []piece
	historySize int
	// base is the number of the first piece in the history: piece number n is history[n-base].
	// It grows every time an old piece is trimmed, so every viewer's cursor (Client.next) stays right.
	base uint64
	// notify is closed on every new piece, to wake the waiting viewers, and replaced by a new one
	notify chan struct{}
	// hasPCR: this stream has a clock; without it there is no pacing (as before)
	hasPCR bool
	// now is the real clock; tests put in a fake one
	now func() time.Time
	// clock knows each piece's time. Only readLoop uses it (through broadcast), so it needs no mu.
	clock tsClock
	// blindFails: how many times in a row looking for the repeat failed without a usable clock.
	// Only readLoop uses it, so it needs no mu.
	blindFails int
	// failStreak: how many times in a row the channel was cut off in the middle of its replay. From it
	// SlotHold grows on the next turn (slotHold). Only readLoop uses it, so it needs no mu.
	failStreak int
	// openFailed: the last attempt to open the upstream failed (refused, or no answer in
	// OpenTimeout). Until an attempt succeeds the channel waits at the back of the queue, so a
	// channel the provider will not serve cannot keep everyone else from their turn.
	openFailed atomic.Bool
	// struggling: failStreak reached StruggleFails, so the channel yields its turn to the others
	// while they have less than StruggleYield seconds left. Set before each turn (acquireSlot);
	// atomic because the lock reads it from another goroutine (cushion).
	struggling atomic.Bool
	// onOpenFail tells the hub that the first connection failed (for DeadCooldown); may be nil
	onOpenFail func(key string)

	ctx    context.Context
	cancel context.CancelFunc

	stats ChannelStats
}

// piece is one piece of the stream in the history
type piece struct {
	data     []byte
	at       float64 // stream time at the end of this piece, in seconds (from tsClock)
	keyframe bool    // did a packet with the start of a keyframe complete in it?
}

// ChannelStats are the channel's stats
type ChannelStats struct {
	TotalBytesSent uint64
	TotalClients   uint64
	ReconnectCount uint64
	CurrentClients int
	StartedAt      time.Time
}

func newChannel(key string, open OpenFunc, onClose func(*Channel)) *Channel {
	ctx, cancel := context.WithCancel(context.Background())
	return &Channel{
		key:     key,
		open:    open,
		onClose: onClose,
		ready:   make(chan struct{}),
		notify:  make(chan struct{}),
		now:     time.Now,
		clients: make(map[*Client]struct{}),
		ctx:     ctx,
		cancel:  cancel,
		stats:   ChannelStats{StartedAt: time.Now()},
	}
}

// addClient adds a new client and gives it the pre-roll. It returns nil if the channel is closed.
func (ch *Channel) addClient() *Client {
	c := newClient(strconv.FormatUint(clientSeq.Add(1), 10), ch)

	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil
	}
	start := "live edge"
	// The cursor goes at the end of the history, and the data from before (keyframe or pre-roll) in head.
	// Both happen under mu, like broadcast, so no piece is lost or repeated between them.
	c.next = ch.base + uint64(len(ch.history))
	if j, back, ok := ch.joinPoint(); ok {
		// with pacing: from a keyframe JoinBack back, so it has protection
		c.next = j
		start = "keyframe " + strconv.FormatFloat(back, 'f', 0, 64) + "s back (paced)"
	} else if PreRollBytes > 0 {
		c.head = ch.preRoll()
	} else if b := ch.keyframeStart(); b != nil {
		// the player can show a picture right away instead of waiting for the next keyframe
		c.head = [][]byte{b}
		start = "keyframe " + strconv.Itoa(len(b)) + " bytes back"
	}
	ch.clients[c] = struct{}{}
	ch.stats.CurrentClients = len(ch.clients)
	ch.stats.TotalClients++
	total := len(ch.clients)
	ch.mu.Unlock()

	log.Printf("[channel %s] client %s joined (total: %d, starts at %s)", ch.key, c.id, total, start)
	return c
}

// nextFor returns the next piece for viewer c. If there is none yet, it returns wait, which
// is closed when a new piece arrives. If the viewer fell so far behind that its piece was trimmed
// from the history, it skips ahead to the oldest keyframe left.
// With pacing: if the piece is there but further than the player's position + PaceLead, we do not
// return it, and return until (when its time comes).
func (ch *Channel) nextFor(c *Client) (chunk []byte, wait <-chan struct{}, until time.Time, err error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if len(c.head) > 0 {
		chunk = c.head[0]
		c.head[0] = nil
		c.head = c.head[1:]
		return chunk, nil, time.Time{}, nil
	}
	if c.next < ch.base {
		lost := ch.base - c.next
		c.next = ch.oldestKeyframe()
		c.anchored = false // skipped ahead in the stream: its old clock no longer fits
		log.Printf("[channel %s] client %s fell %d pieces behind the buffer, skipping ahead", ch.key, c.id, lost)
	}
	if i := c.next - ch.base; i < uint64(len(ch.history)) {
		p := ch.history[i]
		if Pacing && ch.hasPCR {
			if until, early := ch.pace(c, p.at); early {
				return nil, nil, until, nil
			}
		}
		c.next++
		return p.data, nil, time.Time{}, nil
	}
	if ch.closed {
		return nil, nil, time.Time{}, io.EOF
	}
	return nil, ch.notify, time.Time{}, nil
}

// paceStallSlack: if the player has had no data for more than this many seconds, it has stopped.
// Less than this is just a small delay (the timer, or the gap between two pieces).
const paceStallSlack = 1.0

// pace decides whether it is too early to send a piece with time `at` to viewer c, and if so until when.
// The caller holds mu.
func (ch *Channel) pace(c *Client, at float64) (until time.Time, early bool) {
	now := ch.now()
	if !c.anchored {
		// first piece: the player starts here
		c.anchored, c.playAt, c.playWall, c.sent = true, at, now, false
	}
	pos := c.playAt + now.Sub(c.playWall).Seconds()
	lead := PaceLead.Seconds()
	if hole := at - c.lastAt; c.sent && hole > paceStallSlack {
		// The stream's own clock jumped ahead: do not make the player wait through the gap (it used
		// to wait, think after a second that the player had stopped, set its clock back, and so on forever)
		c.playAt += hole
		pos += hole
	} else if c.sent && pos-c.lastAt > paceStallSlack {
		// The player used up everything we sent more than a second ago, so it stopped (the stream did not
		// arrive in time). When the stream comes back it goes on from where it stopped, so we set its clock
		// there instead of sending it everything at once.
		log.Printf("[channel %s] client %s ran out of stream for %.1fs (buffer didn't cover the gap), resuming",
			ch.key, c.id, pos-c.lastAt)
		c.playAt, c.playWall, pos = c.lastAt, now, c.lastAt
	}
	if at > pos+lead {
		wait := time.Duration((at - lead - c.playAt) * float64(time.Second))
		return c.playWall.Add(wait), true
	}
	c.lastAt, c.sent = at, true
	return time.Time{}, false
}

// joinPoint, with pacing: the piece a new viewer on a running channel starts from, that is the
// last keyframe at least JoinBack seconds before the newest data (one more piece back, because
// the start of the packet may be in it), and how many seconds back that is.
// ok=false if there is no pacing or no keyframe far enough back. The caller holds mu.
func (ch *Channel) joinPoint() (seq uint64, back float64, ok bool) {
	if !Pacing || !ch.hasPCR || len(ch.history) == 0 {
		return 0, 0, false
	}
	newest := ch.history[len(ch.history)-1].at
	j := -1
	for i, p := range ch.history {
		if p.at > newest-JoinBack {
			break
		}
		if p.keyframe {
			j = i
		}
	}
	if j < 0 {
		return 0, 0, false
	}
	return ch.base + uint64(max(0, j-1)), newest - ch.history[j].at, true
}

// oldestKeyframe is the number of the piece before the oldest piece marked keyframe (because the
// start of the packet may be in it), or the first piece if there is none. The caller holds mu.
func (ch *Channel) oldestKeyframe() uint64 {
	for i, p := range ch.history {
		if p.keyframe {
			return ch.base + uint64(max(0, i-1))
		}
	}
	return ch.base
}

// preRoll returns the newest pieces of the history (not the oldest) up to PreRollBytes, so the
// client starts close to live without gaps. The caller holds mu.
func (ch *Channel) preRoll() [][]byte {
	i, size := len(ch.history), 0
	for i > 0 && len(ch.history)-i < ClientBufSize/2 && size+len(ch.history[i-1].data) <= PreRollBytes {
		i--
		size += len(ch.history[i].data)
	}
	out := make([][]byte, 0, len(ch.history)-i)
	for _, p := range ch.history[i:] {
		out = append(out, p.data)
	}
	return out
}

// removeClient removes a client and closes the channel if it stays empty after IdleTimeout
func (ch *Channel) removeClient(c *Client) {
	ch.mu.Lock()
	_, ok := ch.clients[c]
	if ok {
		delete(ch.clients, c)
		ch.stats.CurrentClients = len(ch.clients)
	}
	empty := len(ch.clients) == 0
	closed := ch.closed
	ch.mu.Unlock()

	c.Close()

	if ok && empty && !closed {
		log.Printf("[channel %s] last client left, will close after idle timeout", ch.key)
		// (if a new client comes before the timeout, idleWatch does not close it)
		// (IdleTimeout is read here, not in the goroutine, so tests can change it safely)
		go ch.idleWatch(IdleTimeout)
	}
}

// idleWatch closes the channel if it is still empty after timeout
func (ch *Channel) idleWatch(timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ch.ctx.Done():
		return
	}

	ch.mu.Lock()
	empty := len(ch.clients) == 0
	ch.mu.Unlock()

	if empty {
		log.Printf("[channel %s] idle timeout reached, closing", ch.key)
		ch.close()
	}
}

// close closes the channel, the upstream and every client, and removes it from the hub
func (ch *Channel) close() {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return
	}
	ch.closed = true
	if ch.upstream != nil {
		ch.upstream.Close()
	}
	for c := range ch.clients {
		c.Close()
	}
	ch.clients = map[*Client]struct{}{}
	ch.stats.CurrentClients = 0
	stats := ch.stats
	ch.mu.Unlock()

	ch.cancel()

	if ch.onClose != nil {
		ch.onClose(ch)
	}
	log.Printf("[channel %s] closed (total bytes: %d, total clients: %d)",
		ch.key, stats.TotalBytesSent, stats.TotalClients)
}

// openUpstream opens the upstream, but gives up if the provider has not answered within
// OpenTimeout (the connection lock is held meanwhile). The connection's context lives until the
// returned stream is closed. It records whether the attempt failed (openFailed).
func (ch *Channel) openUpstream() (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ch.ctx)
	timer := time.AfterFunc(OpenTimeout, cancel)
	up, err := ch.open(ctx)
	if !timer.Stop() && err == nil {
		// the answer came only after the timeout fired: treat it as no answer
		up.Close()
		err = context.DeadlineExceeded
	}
	if err != nil {
		cancel()
		ch.openFailed.Store(true)
		if errors.Is(err, context.Canceled) && ch.ctx.Err() == nil {
			err = fmt.Errorf("provider did not answer within %s", OpenTimeout)
		}
		return nil, err
	}
	ch.openFailed.Store(false)
	return &cancelOnClose{ReadCloser: up, cancel: cancel}, nil
}

// cancelOnClose ends the connection's context when the stream is closed
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// run is the main loop: it opens the upstream, reads and distributes, and reconnects when cut off
func (ch *Channel) run() {
	// whatever the reason for leaving, the channel must be removed from the hub
	// (otherwise new clients get stuck on a dead channel)
	defer ch.close()

	release, ok := ch.acquireSlot()
	if !ok {
		ch.startErr = ch.ctx.Err()
		close(ch.ready)
		return
	}
	up, err := ch.openUpstream()
	if err != nil {
		release()
		log.Printf("[channel %s] could not open: %v", ch.key, err)
		if ch.onOpenFail != nil {
			ch.onOpenFail(ch.key)
		}
		ch.startErr = err
		close(ch.ready)
		return
	}
	if !ch.setUpstream(up) {
		release()
		close(ch.ready)
		return
	}
	close(ch.ready)

	for reconnected := false; ; reconnected = true {
		ch.readLoop(up, reconnected, release)
		up.Close()

		if ch.isClosed() {
			return
		}

		up, release = ch.reconnect()
		if up == nil {
			log.Printf("[channel %s] giving up on reconnect", ch.key)
			return
		}
	}
}

// acquireSlot waits for the channel's turn on the provider connection. release frees the lock, and is
// safe to call more than once. ok=false if the channel closed while it waited.
// A channel that has not sent a byte yet (new) goes before the others: its viewer sees a black
// screen, while the others have a buffer.
func (ch *Channel) acquireSlot() (release func(), ok bool) {
	if ch.slot == nil {
		return func() {}, true
	}
	struggling := ch.failStreak >= StruggleFails
	if struggling && !ch.struggling.Load() {
		log.Printf("[channel %s] %d failed turns in a row: it waits until the others have %.0fs left",
			ch.key, ch.failStreak, StruggleYield)
	}
	ch.struggling.Store(struggling)
	if !ch.slot.acquireBy(ch.ctx, ch.cushion) {
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(ch.slot.release) }, true
}

// cushion is how many seconds of stream the viewer closest to stopping has left (before what we
// sent runs out), so the lock gives the turn to the most urgent:
//   - a new channel (nothing arrived yet) or a viewer that has not started: 0, black screen
//   - a stream without a PCR or without pacing: as before, after the new ones, in order
//   - a channel nobody watches: last
func (ch *Channel) cushion() float64 {
	// the provider refused it (or did not answer) last time: back of the queue until it works
	if ch.openFailed.Load() {
		return math.Inf(1)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.historySize == 0 {
		return urgentNow
	}
	if !Pacing || !ch.hasPCR {
		return urgentLater
	}
	if len(ch.clients) == 0 {
		return math.Inf(1)
	}
	newest := ch.history[len(ch.history)-1].at
	now := ch.now()
	least := math.Inf(1)
	for c := range ch.clients {
		if !c.anchored {
			return urgentNow
		}
		pos := c.playAt + now.Sub(c.playWall).Seconds()
		least = min(least, newest-pos)
	}
	// a struggling channel comes after every channel with less than StruggleYield left
	// (their urgency is below StruggleYield, its own is at least that much)
	if ch.struggling.Load() {
		return StruggleYield + max(least, 0)
	}
	return least
}

// isNew is true if the channel has not sent anything to the viewers yet
func (ch *Channel) isNew() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.historySize == 0
}

// setUpstream stores the current upstream; it returns false if the channel closed meanwhile
func (ch *Channel) setUpstream(up io.ReadCloser) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		up.Close()
		return false
	}
	ch.upstream = up
	return true
}

// slotHold is how long the channel holds the lock this turn: SlotHold, doubled for each time in
// a row it was cut off in the middle of its replay, up to SlotHoldMax.
func (ch *Channel) slotHold() time.Duration {
	d := SlotHold
	for i := 0; i < ch.failStreak && d < SlotHoldMax; i++ {
		d *= 2
	}
	return min(d, SlotHoldMax)
}

// readLoop reads from the upstream until it is cut off or gets stuck.
// dedupe=true after a reconnect: the provider starts by resending the last few seconds it sent
// before, so we drop that part, so the viewer does not see the stream repeat.
// release frees the connection lock: as soon as we are done with the replay (or the first data if
// there is no replay), or when the connection ends. After that another channel can take its turn.
func (ch *Channel) readLoop(up io.ReadCloser, dedupe bool, release func()) {
	defer release()
	holdFor := ch.slotHold()
	if ch.failStreak > 0 {
		log.Printf("[channel %s] holding the slot up to %s this turn (%d failed in a row)",
			ch.key, holdFor, ch.failStreak)
	}
	hold := time.AfterFunc(holdFor, release)
	defer hold.Stop()
	// watchdog: if the upstream got stuck without closing, we close it to reconnect
	stall := time.AfterFunc(StallTimeout, func() {
		log.Printf("[channel %s] upstream stalled for %s, reconnecting", ch.key, StallTimeout)
		up.Close()
	})
	defer stall.Stop()

	// We keep the last TailBytes we sent and look for them in the new data: everything up to
	// their end is repeated. This way it does not matter how far back the provider goes.
	var tail, pending []byte
	var deadline time.Time
	if dedupe {
		tail = ch.historyTail(TailBytes)
		dedupe = len(tail) == TailBytes
		deadline = time.Now().Add(ReplayWait)
	}
	// overlapChecked: did we compare the first PCR of the new data with our last PCR?
	overlapChecked := false
	// blind: the clock could not tell us whether there is an overlap (no PCR, or the clock made an
	// unbelievable jump). Only here do we count failures for the safety net below.
	blind := false
	// Safety net: if looking for the repeat failed "blind" twice in a row, maybe the last thing
	// we sent is not in the replay at all, and we would keep looking forever. This time we send
	// everything as it is (worst case: a repeated clip once, instead of getting stuck).
	if dedupe && ch.blindFails >= dedupeBlindFails {
		log.Printf("[channel %s] repeat not found %d times in a row without a usable clock, sending the replay as is",
			ch.key, ch.blindFails)
		dedupe, ch.blindFails = false, 0
	}

	// Measurement: on the first connection the provider sends its buffer at once. We measure how much
	// arrived in the first two seconds, and how much in the next 10 (the normal stream speed),
	// and from them how many seconds of stream were in the buffer.
	var burst, steady int
	var measureStart time.Time
	if !dedupe && ch.isNew() { // the channel's first connection, not a reconnect
		measureStart = time.Now()
	}

	buf := make([]byte, ChunkSize)
	for {
		n, err := up.Read(buf)
		data := buf[:n]
		if n > 0 {
			stall.Reset(StallTimeout)
		}
		if !measureStart.IsZero() {
			switch since := time.Since(measureStart); {
			case since < 2*time.Second:
				burst += n
			case since < 12*time.Second:
				steady += n
			default:
				rate := float64(steady) / 10 // bytes per second
				if rate > 0 {
					log.Printf("[channel %s] first connect: %.1fMB in first 2s, then %.2fMB/s (provider buffer ≈ %.0fs)",
						ch.key, float64(burst)/1e6, rate/1e6, float64(burst)/rate)
				}
				measureStart = time.Time{} // measurement done
			}
		}
		if dedupe {
			// search only the new data (with a bit of the old, in case tail is split between them)
			from := max(0, len(pending)-len(tail)+1)
			pending = append(pending, data...)
			// If the gap is bigger than the replay the provider has, the last thing we sent is not
			// in the replay at all, and we kept looking for it forever (and the connection was cut
			// before the backup limit). The clock tells us: if the first new PCR is later than our last
			// PCR, there is no overlap, so everything is new and we send it at once.
			noOverlap, gap := false, 0.0
			if !overlapChecked && ch.clock.seen {
				if first, ok := firstPCR(pending, ch.clock.pcrPID); ok {
					overlapChecked = true
					gap = pcrAfter(first, ch.clock.prev)
					// (a gap bigger than noOverlapMax is not believable: the clock jumped, like the
					// 8.7 hours at the first reconnect of some channels. We do not believe it, and look
					// for the repeat by bytes as before)
					noOverlap = gap > noOverlapSlack && gap < noOverlapMax
					if gap >= noOverlapMax {
						blind = true
						log.Printf("[channel %s] clock jumped %.0fs at reconnect, looking for the repeat by bytes",
							ch.key, gap)
					}
				}
			}
			if noOverlap {
				log.Printf("[channel %s] replay starts %.1fs after what we have: no overlap, sending it all",
					ch.key, gap)
				data, pending, dedupe = pending, nil, false
				ch.blindFails, ch.failStreak = 0, 0
			} else if i := bytes.Index(pending[from:], tail); i >= 0 {
				cut := from + i + len(tail)
				log.Printf("[channel %s] dropping %d repeated bytes after reconnect (buffer %.1fs)",
					ch.key, cut, ch.bufferedSeconds())
				data, pending, dedupe = pending[cut:], nil, false
				ch.blindFails, ch.failStreak = 0, 0
			} else if err != nil {
				// The provider closed while it was still resending the old data, before it reached the point
				// we had got to. If we sent it, the viewer would go back and get a big burst
				// that confuses the player. We drop it, and the next connection looks for the same point.
				log.Printf("[channel %s] upstream ended during replay, discarding %d old bytes", ch.key, len(pending))

				data, pending, dedupe = nil, nil, false
				ch.failStreak++ // on the next turn it holds the lock longer (slotHold)
				// We only count "blind" failures: if the clock said there is an overlap, it is in
				// the replay and we just did not get to it, so we keep looking without sending repeats.
				if blind || !overlapChecked {
					ch.blindFails++
				} else {
					ch.blindFails = 0
				}
			} else if len(pending) > MaxReplayBytes || time.Now().After(deadline) {
				// the provider sent nothing from before; send everything as it is
				log.Printf("[channel %s] no repeat found after reconnect (%d bytes checked)", ch.key, len(pending))
				data, pending, dedupe = pending, nil, false
				ch.blindFails, ch.failStreak = 0, 0
			} else {
				continue
			}
		}
		release()
		if len(data) > 0 {
			// we make a copy because buf is reused
			chunk := make([]byte, len(data))
			copy(chunk, data)
			ch.broadcast(chunk)
		}
		if err != nil {
			if err != io.EOF && !ch.isClosed() {
				log.Printf("[channel %s] upstream read error: %v", ch.key, err)
			}
			return
		}
	}
}

// historyTail returns the last n bytes we sent (or less if we have not sent that much yet)
func (ch *Channel) historyTail(n int) []byte {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	i, size := len(ch.history), 0
	for i > 0 && size < n {
		i--
		size += len(ch.history[i].data)
	}
	tail := make([]byte, 0, size)
	for _, p := range ch.history[i:] {
		tail = append(tail, p.data...)
	}
	if len(tail) > n {
		tail = tail[len(tail)-n:]
	}
	return tail
}

// historyTooLong is true if we can drop the oldest piece: the history after it still holds
// BufferSeconds or more, or it grew past the memory limit. A stream without a PCR falls back to
// the old limit in bytes. The caller holds mu.
func (ch *Channel) historyTooLong() bool {
	if ch.historySize > MaxBufferBytes {
		return true
	}
	if !ch.clock.seen {
		return ch.historySize > HistoryBytes
	}
	return ch.history[len(ch.history)-1].at-ch.history[1].at >= BufferSeconds
}

// bufferedSeconds is how many seconds of stream the history holds (0 without a PCR)
func (ch *Channel) bufferedSeconds() float64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.history) < 2 {
		return 0
	}
	return ch.history[len(ch.history)-1].at - ch.history[0].at
}

// broadcast adds a piece for every client without waiting for anyone
func (ch *Channel) broadcast(chunk []byte) {
	// the clock is outside the lock: only this goroutine uses it
	at, key := ch.clock.feed(chunk)

	ch.mu.Lock()
	ch.history = append(ch.history, piece{data: chunk, at: at, keyframe: key})
	ch.historySize += len(chunk)
	if ch.clock.seen {
		ch.hasPCR = true
	}
	for len(ch.history) > 1 && ch.historyTooLong() {
		ch.historySize -= len(ch.history[0].data)
		ch.history[0] = piece{} // so the GC can free it
		ch.history = ch.history[1:]
		ch.base++
	}
	ch.stats.TotalBytesSent += uint64(len(chunk))
	// We do not send to anyone ourselves: each viewer reads with its cursor (Client.Recv). We only wake
	// the waiting ones that something new arrived.
	wake := ch.notify
	ch.notify = make(chan struct{})
	ch.mu.Unlock()
	close(wake)
}

// reconnect tries to reopen the upstream; the first attempt is immediate (once it is the channel's turn).
// It returns with the lock held, and the caller frees it with release.
func (ch *Channel) reconnect() (io.ReadCloser, func()) {
	for i := 0; i < ReconnectMaxRetries; i++ {
		if i > 0 {
			select {
			case <-ch.ctx.Done():
				return nil, nil
			case <-time.After(ReconnectDelay):
			}
		}

		release, ok := ch.acquireSlot()
		if !ok {
			return nil, nil
		}

		log.Printf("[channel %s] reconnecting (attempt %d/%d)",
			ch.key, i+1, ReconnectMaxRetries)

		up, err := ch.openUpstream()
		if err != nil {
			release()
			if ch.ctx.Err() != nil {
				return nil, nil
			}
			log.Printf("[channel %s] reconnect failed: %v (back of the queue until it works)", ch.key, err)
			// wait a little even with ReconnectDelay 0, so a refused channel does not loop
			select {
			case <-ch.ctx.Done():
				return nil, nil
			case <-time.After(failPause):
			}
			continue
		}
		if !ch.setUpstream(up) {
			release()
			return nil, nil
		}

		ch.mu.Lock()
		ch.stats.ReconnectCount++
		ch.mu.Unlock()

		// how much the viewers had left when the channel's turn came (to see whether the priority works)
		if left := ch.cushion(); left < 0 {
			log.Printf("[channel %s] reconnected successfully (viewers stalled for %.1fs)", ch.key, -left)
		} else if left < urgentLater {
			log.Printf("[channel %s] reconnected successfully (viewers had %.1fs left)", ch.key, left)
		} else {
			log.Printf("[channel %s] reconnected successfully", ch.key)
		}
		return up, release
	}
	return nil, nil
}

// viewerCount is how many clients are watching now
func (ch *Channel) viewerCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.clients)
}

func (ch *Channel) isClosed() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.closed
}

// Stats returns a copy of the stats
func (ch *Channel) Stats() ChannelStats {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.stats
}
