package streamhub

import (
	"bytes"
	"testing"
)

// tsPacket builds one 188-byte TS packet; tag fills the payload so packets
// can be told apart.
func tsPacket(pid int, start, keyframe bool, tag byte) []byte {
	p := bytes.Repeat([]byte{tag}, tsPacketSize)
	p[0] = tsSyncByte
	p[1] = byte(pid>>8) & 0x1f
	if start {
		p[1] |= 0x40
	}
	p[2] = byte(pid)
	p[3] = 0x10 // payload only
	if keyframe {
		p[3] = 0x30 // adaptation field + payload
		p[4] = 1    // adaptation field length
		p[5] = 0x40 // random_access_indicator
	}
	return p
}

// broadcastOddly sends b in pieces that do not line up with packets, the way
// reads from the provider arrive.
func broadcastOddly(ch *Channel, b []byte) {
	for len(b) > 0 {
		n := min(1000, len(b))
		ch.broadcast(append([]byte(nil), b[:n]...))
		b = b[n:]
	}
}

// A viewer joining a running channel starts at the PAT before the last
// keyframe, so the player can show a picture right away.
func TestNewViewerStartsAtLastKeyframe(t *testing.T) {
	withPreRoll(t, 0) // the keyframe start is used when the pre-roll is off
	var ts []byte
	ts = append(ts, 0x12, 0x34) // stray bytes before the first packet
	for i := 0; i < 3; i++ {
		ts = append(ts, tsPacket(0, true, false, 'A'+byte(i))...) // PAT
		ts = append(ts, tsPacket(256, true, true, 'k')...)        // keyframe
		for j := 0; j < 20; j++ {
			ts = append(ts, tsPacket(256, false, false, 'v')...)
		}
	}
	lastPAT := 2 + 2*22*tsPacketSize

	ch := newChannel("k", nil, nil)
	broadcastOddly(ch, ts)

	c := ch.addClient()
	if len(c.head) != 1 {
		t.Fatalf("new viewer got %d chunks, want 1", len(c.head))
	}
	got := c.head[0]
	if !bytes.Equal(got, ts[lastPAT:]) {
		t.Fatalf("new viewer starts %d bytes back, want %d (at the last PAT)", len(got), len(ts)-lastPAT)
	}
}

// Without a keyframe in the history, the viewer starts at the live edge.
func TestNoKeyframeStartsAtLiveEdge(t *testing.T) {
	withPreRoll(t, 0)
	var ts []byte
	for j := 0; j < 50; j++ {
		ts = append(ts, tsPacket(256, false, false, 'v')...)
	}
	ch := newChannel("k", nil, nil)
	broadcastOddly(ch, ts)

	if c := ch.addClient(); len(c.head) != 0 {
		t.Errorf("new viewer got %d chunks without any keyframe", len(c.head))
	}
}
