package main

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"
)

// A fake provider that behaves like the real one this project was built for:
//   - it allows ONE connection at a time: a new connection cuts the previous one off;
//   - on every new connection it first resends the last replaySeconds of the channel at full
//     speed (a burst), then goes on at playback speed.
//
// The streams are MPEG-TS: 188-byte packets, with a PCR (the stream clock) every frame and a
// keyframe every 2 seconds, so the hub's clock, pacing and keyframe code work on them.

const (
	packetSize      = 188
	frameDuration   = 40 * time.Millisecond // one PCR per frame
	packetsPerFrame = 40                    // ~190 KB/s per channel
	keyframeEvery   = 50                    // frames: a keyframe every 2s
	replaySeconds   = 8
	connectDelay    = 300 * time.Millisecond // setting up a connection, like an HTTP request
)

type provider struct {
	start time.Time

	mu     sync.Mutex
	active *upstream // the one connection the account allows
	opens  int
}

func newProvider() *provider {
	// it has been running for a minute already, like a real provider: the first connection gets
	// a full replay burst, not an empty one
	return &provider{start: time.Now().Add(-time.Minute)}
}

type upstream struct {
	pr     *io.PipeReader
	pw     *io.PipeWriter
	cancel context.CancelFunc
}

// Close is the client hanging up.
func (u *upstream) Close() error {
	u.cancel()
	return u.pr.Close()
}

func (u *upstream) Read(p []byte) (int, error) { return u.pr.Read(p) }

// cut is the provider dropping the connection, even in the middle of a write.
func (u *upstream) cut() {
	u.cancel()
	u.pw.CloseWithError(io.ErrUnexpectedEOF)
}

// open connects to a channel. Like the real provider, it cuts whoever was connected before.
func (p *provider) open(ctx context.Context, channel int) (io.ReadCloser, error) {
	// the old connection keeps streaming until the new one is set up
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(connectDelay):
	}

	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	u := &upstream{pr: pr, pw: pw, cancel: cancel}

	p.mu.Lock()
	if p.active != nil {
		p.active.cut() // the account has one connection: the old one is cut off
	}
	p.active = u
	p.opens++
	p.mu.Unlock()

	go p.serve(ctx, u, channel)
	return u, nil
}

func (p *provider) connections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.opens
}

// serve sends the replay burst, then the live stream in real time, until the connection is cut.
func (p *provider) serve(ctx context.Context, u *upstream, channel int) {
	defer u.pw.CloseWithError(io.ErrUnexpectedEOF)

	live := p.frameAt(time.Now())
	frame := max(0, live-replaySeconds*int(time.Second/frameDuration))

	// the replay goes out as a burst, in big writes like a fast network
	var burst []byte
	for ; frame <= live; frame++ {
		burst = append(burst, makeFrame(channel, frame)...)
	}
	for len(burst) > 0 {
		n := min(256*1024, len(burst))
		if _, err := u.pw.Write(burst[:n]); err != nil {
			return
		}
		burst = burst[n:]
	}

	for ; ; frame++ {
		if frame > live {
			// caught up: wait for this frame's time
			due := p.start.Add(time.Duration(frame) * frameDuration)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(due)):
			}
		}
		if ctx.Err() != nil {
			return
		}
		if _, err := u.pw.Write(makeFrame(channel, frame)); err != nil {
			return
		}
	}
}

func (p *provider) frameAt(t time.Time) int {
	return int(t.Sub(p.start) / frameDuration)
}

// makeFrame builds one frame of channel: [PAT if keyframe] PCR packet, payload packets.
// Every payload is unique (channel, frame, packet), so the hub can find where a replay repeats.
func makeFrame(channel, frame int) []byte {
	key := frame%keyframeEvery == 0
	var out []byte
	if key {
		out = append(out, packet(0, nil)...) // PAT, so a new viewer can start here
	}
	out = append(out, pcrPacket(0x100, float64(frame)*frameDuration.Seconds(), key)...)
	for j := 0; j < packetsPerFrame; j++ {
		var tag [12]byte
		binary.BigEndian.PutUint32(tag[0:], uint32(channel))
		binary.BigEndian.PutUint32(tag[4:], uint32(frame))
		binary.BigEndian.PutUint32(tag[8:], uint32(j))
		out = append(out, packet(0x100, tag[:])...)
	}
	return out
}

// packet is a payload-only TS packet carrying tag (repeated to fill it)
func packet(pid int, tag []byte) []byte {
	p := make([]byte, packetSize)
	p[0] = 0x47
	p[1] = byte(pid>>8) & 0x1f
	p[2] = byte(pid)
	p[3] = 0x10 // payload only
	for i := 4; i < packetSize && len(tag) > 0; i += len(tag) {
		copy(p[i:], tag)
	}
	return p
}

// pcrPacket is a TS packet with an adaptation field holding the PCR (and the keyframe flag)
func pcrPacket(pid int, seconds float64, keyframe bool) []byte {
	p := packet(pid, nil)
	p[3] = 0x30 // adaptation field + payload
	p[4] = 7    // flags + 6 PCR bytes
	p[5] = 0x10 // PCR flag
	if keyframe {
		p[5] |= 0x40 // random access indicator
	}
	base := uint64(seconds*90_000) % (1 << 33)
	p[6] = byte(base >> 25)
	p[7] = byte(base >> 17)
	p[8] = byte(base >> 9)
	p[9] = byte(base >> 1)
	p[10] = byte(base&1)<<7 | 0x7e
	p[11] = 0
	return p
}

// readPCR returns the PCR of a packet in seconds, if it has one
func readPCR(p []byte) (float64, bool) {
	if len(p) < 12 || p[0] != 0x47 || p[3]&0x20 == 0 || p[4] < 7 || p[5]&0x10 == 0 {
		return 0, false
	}
	base := uint64(p[6])<<25 | uint64(p[7])<<17 | uint64(p[8])<<9 | uint64(p[9])<<1 | uint64(p[10])>>7
	return float64(base) / 90_000, true
}
