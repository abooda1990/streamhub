package streamhub

// Stream clock: every few MPEG-TS packets carry a PCR, meaning "this packet's time
// in the stream is X", on a 27MHz clock. From it we know how many seconds of stream the history holds.
const (
	pcrHz = 27_000_000

	pcrWrap = float64(uint64(1)<<33*300) / pcrHz
	// A jump forward bigger than this (or any step back) is a new start in the stream,
	// not real time that passed: we keep counting from the last time we had
	// (it was 10: 5-second steps in the stream counted as real time, so pacing got stuck
	// and the history was trimmed wrongly. The spec says a PCR every 100ms at most, so
	// anything bigger than one second is not time that passed.)
	pcrMaxJump = 1.0
)

// tsClock reads the stream packet by packet as it arrives. Pieces from the provider do not
// split on packet boundaries, so we keep the half packet at the end for the next piece.
// Only the goroutine reading from the provider uses it, so it needs no lock.
type tsClock struct {
	carry  []byte  // half a packet from the end of the previous piece
	seen   bool    // is there any PCR in this stream?
	pcrPID int     // the PID that carries the PCR (we stick to one)
	prev   float64 // the last PCR we read, in seconds
	now    float64 // stream time since the first PCR, in seconds (always forward)
}

// feed reads the piece and returns the stream time at its end, and whether a packet with
// the start of a keyframe completed in it.
func (c *tsClock) feed(chunk []byte) (at float64, keyframe bool) {
	b := chunk
	if len(c.carry) > 0 {
		b = append(append(make([]byte, 0, len(c.carry)+len(chunk)), c.carry...), chunk...)
	}

	p := 0
	for p+tsPacketSize <= len(b) {
		if b[p] != tsSyncByte {
			// lost sync (for example after a reconnect): find the next 0x47
			p++
			continue
		}
		pkt := b[p : p+tsPacketSize]
		if pcr, ok := readPCR(pkt); ok {
			c.observe(pktPID(pkt), pcr)
		}
		if isRandomAccess(pkt) {
			keyframe = true
		}
		p += tsPacketSize
	}
	c.carry = append(c.carry[:0:0], b[p:]...)
	return c.now, keyframe
}

// observe moves the clock on a new PCR
func (c *tsClock) observe(pid int, pcr float64) {
	if !c.seen {
		c.seen, c.pcrPID, c.prev = true, pid, pcr
		return
	}
	if pid != c.pcrPID {
		return
	}
	d := pcr - c.prev
	if d < -pcrWrap/2 {
		d += pcrWrap // the clock wrapped back to zero
	}
	if d < 0 || d > pcrMaxJump {
		d = 0 // a new start in the stream: do not jump
	}
	c.now += d
	c.prev = pcr
}

func readPCR(pkt []byte) (float64, bool) {
	// the adaptation field is there, long enough for the PCR (flags + 6 bytes), and PCR_flag is set
	if pkt[3]&0x20 == 0 || pkt[4] < 7 || pkt[5]&0x10 == 0 {
		return 0, false
	}
	b := pkt[6:12]
	base := uint64(b[0])<<25 | uint64(b[1])<<17 | uint64(b[2])<<9 |
		uint64(b[3])<<1 | uint64(b[4])>>7
	ext := uint64(b[4]&0x01)<<8 | uint64(b[5])
	return float64(base*300+ext) / pcrHz, true
}

// pktPID is the packet's PID (13 bits)
func pktPID(pkt []byte) int {
	return int(pkt[1]&0x1f)<<8 | int(pkt[2])
}

// noOverlapSlack: the new data must start later than the last thing we have by more than this
// to count as no overlap (a margin for the distance between two PCRs).
const noOverlapSlack = 0.5

// noOverlapMax: a gap bigger than this between the last thing we have and the first new thing is
// not believable (no absence is that long), so the clock jumped. We do not believe it.
const noOverlapMax = 300.0

// dedupeBlindFails: after this many failures in a row looking for the repeat without a clock,
// we send the replay as it is (safety net)
const dedupeBlindFails = 2

// firstPCR is the first PCR (in seconds) on the given PID in b, if there is one.
func firstPCR(b []byte, pid int) (float64, bool) {
	for p := 0; p+tsPacketSize <= len(b); {
		if b[p] != tsSyncByte {
			p++
			continue
		}
		pkt := b[p : p+tsPacketSize]
		if pktPID(pkt) == pid {
			if pcr, ok := readPCR(pkt); ok {
				return pcr, true
			}
		}
		p += tsPacketSize
	}
	return 0, false
}

// pcrAfter is how many seconds a is after b, taking the clock wrap into account (negative = a before b)
func pcrAfter(a, b float64) float64 {
	d := a - b
	if d < -pcrWrap/2 {
		d += pcrWrap
	} else if d > pcrWrap/2 {
		d -= pcrWrap
	}
	return d
}
