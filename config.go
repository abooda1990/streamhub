package streamhub

import "time"

// General settings
const (
	// ChunkSize is the size of one read from the upstream
	ChunkSize = 64 * 1024 // 64 KiB

	// ClientBufSize was the number of pieces in each client's buffer. Now (no queue per
	// client) it only limits the pre-roll. Reads from the upstream are usually small (a few KB),
	// so the number had to be big to cover a few seconds. Pieces are shared, so it costs only pointers.
	ClientBufSize = 1024

	// HistoryBytes is how much of the latest stream we keep (for the keyframe and the dedupe).
	// It must reach the last keyframe: on high-bitrate streams it can be 2-4 seconds back.
	// Now only for streams without a PCR; otherwise the limit is in seconds (BufferSeconds).
	HistoryBytes = 8 * 1024 * 1024

	// BufferSeconds is how many seconds of stream we keep per channel. We first measured ~40s
	// sent on the first connection (by bytes: 38-40s; by PCR it is ~16-27s), and keep this much for pacing.
	BufferSeconds = 40.0

	// MaxBufferBytes is a memory safety limit per channel, in case the stream is very fast
	// or the PCR is wrong
	MaxBufferBytes = 64 * 1024 * 1024

	// TailBytes is how much of the last data we sent we remember, and look for in the new data
	// after a reconnect. Big enough to be unique (not repeated padding).
	TailBytes = 32 * 1024

	// MaxReplayBytes and ReplayWait: how far we keep looking for the repeat after a reconnect
	// before we give up and send the data as it is. The provider usually resends 25-32MB, and loading
	// it can take more than 4 seconds, so the main limit is the size, and the time is only a backup.
	MaxReplayBytes = 48 * 1024 * 1024
	ReplayWait     = 15 * time.Second

	// ReconnectDelay is how long we wait between attempts to reopen the upstream (the first one is immediate)
	ReconnectDelay = 1 * time.Second

	// ReconnectMaxRetries is the maximum number of attempts before giving up on the channel
	ReconnectMaxRetries = 8

	// failPause: after a failed attempt to open the upstream we wait at least this long before the
	// next one (even with ReconnectDelay 0), so a channel the provider refuses does not loop.
	failPause = 1 * time.Second

	// StallTimeout: if the upstream sends no byte for this long, we treat it as stuck and reopen it
	StallTimeout = 15 * time.Second

	// WriteTimeoutPerChunk is the write deadline for each piece to a client
	WriteTimeoutPerChunk = 10 * time.Second
)

// IdleTimeout is how long we wait before closing a channel with no clients. Short, because the
// provider account is limited and an empty channel takes turns from the watched ones, but not zero:
// a player that reconnects comes back within a second or two and must find the same channel.
// A variable, not a constant, so tests can shorten it.
var IdleTimeout = 3 * time.Second

// SlotHold is the longest a channel holds the provider connection lock while it waits for its
// replay to finish. The replay usually finishes in under a second; the limit is so a dead channel (the
// provider accepted the connection and sent nothing) does not hold up every channel until StallTimeout.
// A variable, not a constant, so tests can shorten it.
var SlotHold = 2 * time.Second

// SlotHoldMax: a channel cut off in the middle of its replay holds the lock on its next turn
// twice as long (SlotHold × 2 for each failure in a row) up to this limit, so a heavy channel
// gets to finish. As soon as it succeeds it goes back to SlotHold.
var SlotHoldMax = 6 * time.Second

// StruggleFails and StruggleYield: a channel cut off in the middle of its replay StruggleFails
// times in a row (its provider source is too slow for its turn) gets the lock only when no other
// waiting channel has less than StruggleYield seconds left. A failed turn costs the others its
// whole hold and brings no stream, so the healthy channels must not pay for it.
var StruggleFails = 2
var StruggleYield = 10.0

// On-demand downloads (VOD: a recorded file rather than a live stream) can share the provider
// connection with the live channels through Hub.AcquireSlot. Their client usually buffers a minute
// or more, so they can wait; a live viewer has only our buffer.
//   - VODUrgency: a download gets its turn after every live channel with less than this many
//     seconds left, and before the ones with more.
//   - VODHold: how long a download holds the lock on its turn. After that it may keep reading until
//     the next one connects (the provider then cuts it) and go on from there (Range) on its next turn.
//   - VODMaxFails: how many failed attempts in a row to go on with a download before giving up.
var VODUrgency = 15.0
var VODHold = 3 * time.Second
var VODMaxFails = 5

// OpenTimeout: how long we wait for the provider to answer while we hold the connection lock.
// A channel that is offline (e.g. an event channel with no event) may not answer at all; without
// this the whole lock waited for the HTTP timeout (30s) and every other channel stalled.
var OpenTimeout = 5 * time.Second

// DeadCooldown: a channel whose first connection failed (the provider refused it or did not
// answer) starts at the back of the queue if it is opened again within this time.
var DeadCooldown = 30 * time.Second

// PreRollBytes is how much of the latest stream we send a new client, from a random point.
// Off (0): players that reconnect used to see the stream go back.
// Instead we send from the last keyframe (see keyframeStart).
var PreRollBytes = 0

// Pacing: each viewer gets the stream at playback speed (by the PCR) from the history, instead of
// each piece as soon as it arrives. This way the history (BufferSeconds) protects it from the gaps
// between the channels' turns on the provider connection. false makes it work as before.
var Pacing = true

// PaceLead is how far ahead of the player's position we send. It fills the player's buffer, and
// when a viewer joins it goes out at once, so the picture shows up fast.
var PaceLead = 4 * time.Second

// JoinBack: a viewer joining a running channel starts at a keyframe this many
// seconds before the newest data, so it has the same protection as the others.
var JoinBack = 30.0
