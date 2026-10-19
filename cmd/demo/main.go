// Command demo shows the problem streamhub solves, and how it solves it, against a fake provider
// that allows only one connection at a time (see provider.go).
//
//	go run ./cmd/demo                        # both modes, one after the other
//	go run ./cmd/demo -mode hub -channels 4  # only through the hub
//	go run ./cmd/demo -v                     # also print the hub's own log
//
// Each viewer is a simulated player: it plays the stream in real time by its clock (PCR) and
// stalls when it runs out of data. At the end the demo prints how long every viewer was stalled.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"streamhub"
)

func main() {
	mode := flag.String("mode", "both", "direct, hub or both")
	channels := flag.Int("channels", 3, "number of channels")
	viewers := flag.Int("viewers", 2, "viewers per channel")
	duration := flag.Duration("duration", 40*time.Second, "how long each mode runs")
	verbose := flag.Bool("v", false, "print the hub's log")
	flag.Parse()

	if !*verbose {
		log.SetOutput(io.Discard)
	}

	fmt.Printf("fake provider: 1 connection allowed, %ds replay burst on every connect\n", replaySeconds)
	fmt.Printf("%d channels x %d viewers, %s per mode\n\n", *channels, *viewers, *duration)

	var results []result
	if *mode == "direct" || *mode == "both" {
		results = append(results, run("direct", *channels, *viewers, *duration))
	}
	if *mode == "hub" || *mode == "both" {
		results = append(results, run("hub", *channels, *viewers, *duration))
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\nmode\tviewers\tprovider connections\tjumps back\tseconds shown twice\tstalls\ttime stalled")
	for _, r := range results {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%.0fs\t%d\t%.1fs\n",
			r.mode, r.viewers, r.connections, r.jumps, r.repeated, r.stalls, r.stalled.Seconds())
	}
	w.Flush()
}

type result struct {
	mode        string
	viewers     int
	connections int
	jumps       int     // times the picture went back (a replay reached the player)
	repeated    float64 // seconds of stream the viewers were shown a second time
	stalls      int
	stalled     time.Duration
}

// run starts every viewer in one mode and measures them
func run(mode string, channels, perChannel int, d time.Duration) result {
	fmt.Printf("--- %s: ", mode)
	if mode == "direct" {
		fmt.Println("every viewer opens its own provider connection")
	} else {
		fmt.Println("viewers go through the hub: one connection, taken in turns")
	}

	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	prov := newProvider()
	hub := streamhub.NewHub()
	var players []*player
	var wg sync.WaitGroup
	for c := 1; c <= channels; c++ {
		for v := 0; v < perChannel; v++ {
			p := &player{name: fmt.Sprintf("ch%d/v%d", c, v+1)}
			players = append(players, p)
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				if mode == "direct" {
					watchDirect(ctx, prov, c, p)
				} else {
					watchHub(ctx, hub, prov, c, p)
				}
			}(c)
		}
	}

	// the players' clocks, and a progress line every 5 seconds
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	last := start
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case now := <-tick.C:
			for _, p := range players {
				p.tick(now)
			}
			if now.Sub(last) >= 5*time.Second {
				last = now
				fmt.Printf("  %3.0fs  %s\n", now.Sub(start).Seconds(), progress(players))
			}
		}
	}
	wg.Wait()

	r := result{mode: mode, viewers: len(players), connections: prov.connections()}
	for _, p := range players {
		p.mu.Lock()
		r.jumps += p.jumps
		r.repeated += p.repeated
		r.stalls += p.stalls
		r.stalled += p.stalled
		p.mu.Unlock()
	}
	return r
}

// watchDirect: the viewer connects to the provider itself, and reconnects when it is cut off
// (after a short pause, like a real player). Every viewer cuts the others off.
func watchDirect(ctx context.Context, prov *provider, channel int, p *player) {
	for ctx.Err() == nil {
		up, err := prov.open(ctx, channel)
		if err != nil {
			return
		}
		buf := make([]byte, 64*1024)
		for {
			n, err := up.Read(buf)
			p.feed(buf[:n])
			if err != nil {
				break
			}
		}
		up.Close()
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// watchHub: the viewer joins the channel through the hub
func watchHub(ctx context.Context, hub *streamhub.Hub, prov *provider, channel int, p *player) {
	key := fmt.Sprintf("live/%d", channel)
	open := func(ctx context.Context) (io.ReadCloser, error) { return prov.open(ctx, channel) }
	client, err := hub.Subscribe(ctx, key, open)
	if err != nil {
		return
	}
	defer hub.Unsubscribe(client)
	for {
		chunk, err := client.Recv(ctx)
		if err != nil {
			return
		}
		p.feed(chunk)
	}
}

// player is a simulated video player: it plays from the first stream time it got, in real
// time, and stalls when the newest stream time it has is behind its position. A real player
// also shows whatever it receives in order, so data from the past (a replay) makes the picture
// jump back: we count those jumps and how many seconds were shown twice.
type player struct {
	name string

	mu       sync.Mutex
	carry    []byte
	newest   float64 // newest stream time received (PCR, seconds)
	last     float64 // stream time of the last packet received, in arrival order
	pos      float64 // stream time being shown
	started  bool
	lastAt   time.Time
	jumps    int
	repeated float64
	stalled  time.Duration
	stalls   int
	inStall  bool
}

func (p *player) feed(b []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.carry = append(p.carry, b...)
	for len(p.carry) >= packetSize {
		if p.carry[0] != 0x47 {
			p.carry = p.carry[1:] // resync
			continue
		}
		if t, ok := readPCR(p.carry[:packetSize]); ok {
			if !p.started {
				p.started, p.pos, p.newest, p.last = true, t, t, t
			}
			if t < p.last-0.5 {
				// the stream went back in time: everything from t to newest is shown again
				p.jumps++
				p.repeated += p.newest - t
			}
			p.last = t
			p.newest = max(p.newest, t) // old data (a replay) does not move it
		}
		p.carry = p.carry[packetSize:]
	}
}

func (p *player) tick(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		p.lastAt = now
		return
	}
	dt := now.Sub(p.lastAt)
	p.lastAt = now
	if p.newest-p.pos > 0 {
		p.pos = min(p.pos+dt.Seconds(), p.newest)
		p.inStall = false
		return
	}
	p.stalled += dt
	if !p.inStall {
		p.stalls++
		p.inStall = true
	}
}

// progress is one line: each viewer's buffer, or STALL
func progress(players []*player) string {
	var parts []string
	for _, p := range players {
		p.mu.Lock()
		s := fmt.Sprintf("%s %4.1fs", p.name, p.newest-p.pos)
		if p.inStall {
			s = fmt.Sprintf("%s STALL", p.name)
		}
		p.mu.Unlock()
		parts = append(parts, s)
	}
	return strings.Join(parts, " | ")
}
