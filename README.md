# streamhub

[![test](https://github.com/abooda1990/streamhub/actions/workflows/test.yml/badge.svg)](https://github.com/abooda1990/streamhub/actions/workflows/test.yml)

Share **one upstream connection** between many live streams and many viewers, in Go.

Some live sources allow only one connection at a time: every new connection cuts the previous
one, and on every connect the source first resends the last few seconds it sent. `streamhub`
sits in front of such a source and lets many viewers watch several streams through that one
connection, without stalls and without seeing the picture jump back.

I built it as a learning project, to practise concurrency and streaming in Go on a real problem.
The numbers below come from a fake source that behaves like the real one.

## Try it

```bash
go run ./cmd/demo
```

The demo starts a fake source (one connection allowed, 8 s replay on every connect) and six
simulated players on three channels. It runs them first **directly** against the source, then
**through the hub**:

```
mode    viewers  provider connections  jumps back  seconds shown twice  stalls  time stalled
direct  6        190                   47          246s                 0       0.0s
hub     6        94                    0           0s                   0       0.0s
```

Directly, every viewer cuts the others off, and each reconnect replays seconds the viewer has
already seen. Through the hub, every viewer sees a continuous stream.

Flags: `-channels 4 -viewers 3 -duration 60s`, and `-v` to print the hub's own log.

## How it works

```
 viewers                      streamhub                              source (1 connection)
 ───────                      ─────────                              ─────────────────────
 v1 ──┐                  ┌───────────────────┐
 v2 ──┼── channel 1 ────►│ history (~40 s)   │──┐
 v3 ──┘                  └───────────────────┘  │   slot: one channel
 v4 ───── channel 2 ────►│ history (~40 s)   │──┼── connects at a time, ─────► GET stream
 v5 ───── channel 3 ────►│ history (~40 s)   │──┘   most urgent first
                         └───────────────────┘
```

| Piece | File | What it does |
|---|---|---|
| Fan-out | `hub.go`, `channel.go`, `client.go` | One upstream per channel. Each viewer has its own cursor into a shared history, so a slow viewer holds nobody back. |
| Connection slot | `slot.go` | A lock over the single connection. When it frees up it goes to the channel whose viewers are closest to running out. |
| Stream clock | `tsclock.go` | Reads the PCR (the clock inside MPEG-TS packets), so the history is measured in seconds and survives clock jumps and wrap. |
| Pacing | `channel.go` (`pace`) | Sends each viewer the stream at playback speed, so the history covers the gaps while other channels have the connection. |
| Replay dedupe | `channel.go` (`readLoop`) | After a reconnect, finds where the old data ends (a byte tail, plus a PCR check when the gap is longer than the replay) and drops the repeat. |
| Keyframe start | `keyframe.go` | A new viewer starts at a keyframe, so the picture shows at once. |
| Failure handling | `channel.go`, `hub.go` | A source that does not answer gives up after a timeout and goes to the back of the queue. A channel whose replay keeps getting cut stops taking turns from healthy ones. |

## Use it

```go
hub := streamhub.NewHub()

open := func(ctx context.Context) (io.ReadCloser, error) {
	// open the upstream for this channel, e.g. an HTTP GET
}

client, err := hub.Subscribe(ctx, "live/1", open)
if err != nil {
	return err
}
defer hub.Unsubscribe(client)

for {
	chunk, err := client.Recv(ctx)
	if err != nil {
		return err
	}
	w.Write(chunk) // e.g. to an HTTP response
}
```

The tuning knobs (buffer length, slot hold, pacing lead, ...) are package variables in
`config.go`, each with a comment that says why it has its value.

## Tests

```bash
go test ./...
```

The tests use fake sources, so they need no network. They cover the slot priority, pacing,
the PCR clock and its wrap, replay dedupe (including replays cut short and clock jumps),
keyframe start and dead sources.

## License

MIT
