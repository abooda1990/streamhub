package streamhub

// MPEG-TS: the stream is split into packets of 188 bytes each, starting with 0x47.
const (
	tsPacketSize = 188
	tsSyncByte   = 0x47

	// keyframeLookBack is how far back we go before the keyframe piece to find the PAT.
	// The PAT repeats every ~100ms, so 512KB (about one second of stream) is enough.
	keyframeLookBack = 512 * 1024
)

// keyframeStart returns the end of the stream from the last keyframe until now, with the PAT before it
// (the table the player needs to know where the audio and video are). This way a new viewer
// shows a picture right away instead of waiting for the next keyframe, and goes back only
// a few seconds at most. It returns nil if there is no keyframe in the history.
// The caller holds mu.
func (ch *Channel) keyframeStart() []byte {
	// The history is now ~40 seconds (16-35MB): we do not join it all for every new viewer.
	// We go back to the last piece marked keyframe, and from there keyframeLookBack further so
	// the PAT before it is inside, and we search only that part.
	i := len(ch.history) - 1
	for i >= 0 && !ch.history[i].keyframe {
		i--
	}
	if i < 0 {
		return nil
	}
	for back := 0; i > 0 && back < keyframeLookBack; {
		i--
		back += len(ch.history[i].data)
	}
	size := 0
	for _, p := range ch.history[i:] {
		size += len(p.data)
	}
	hist := make([]byte, 0, size)
	for _, p := range ch.history[i:] {
		hist = append(hist, p.data...)
	}

	lastPAT, start := -1, -1
	for p := tsAlign(hist, 0); p >= 0 && p+tsPacketSize <= len(hist); p += tsPacketSize {
		if hist[p] != tsSyncByte {
			// lost sync (for example after a reconnect); find the first packet after it
			if p = tsAlign(hist, p); p < 0 || p+tsPacketSize > len(hist) {
				break
			}
		}
		pkt := hist[p : p+tsPacketSize]
		pid := int(pkt[1]&0x1f)<<8 | int(pkt[2])
		if pid == 0 && pkt[1]&0x40 != 0 { // start of a PAT
			lastPAT = p
		}
		if isRandomAccess(pkt) {
			start = p
			if lastPAT >= 0 {
				start = lastPAT
			}
		}
	}
	if start < 0 {
		return nil
	}
	return hist[start:]
}

// isRandomAccess is true if a keyframe starts in the packet: the encoder sets
// random_access_indicator in the adaptation field.
func isRandomAccess(pkt []byte) bool {
	hasAdaptation := pkt[3]&0x20 != 0
	return hasAdaptation && pkt[4] > 0 && pkt[5]&0x40 != 0
}

// tsAlign returns the first position from `from` on where 3 packets in a row start with 0x47,
// or -1 if there is none.
func tsAlign(b []byte, from int) int {
	for i := from; i+2*tsPacketSize < len(b); i++ {
		if b[i] == tsSyncByte && b[i+tsPacketSize] == tsSyncByte && b[i+2*tsPacketSize] == tsSyncByte {
			return i
		}
	}
	return -1
}
