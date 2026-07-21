package main

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// XferProgress tracks a push or pull against the transferred tree's total
// footprint (the root key's logical length, known before the first round)
// and renders a one-line bar. Deduplication makes the footprint an upper
// bound: objects the peer already has never cross the wire, so a transfer
// can complete with the bar short of full — Finish clamps it for the
// final frame, and the summary line reports what actually moved.
// It implements wantsync.Progress; a nil *XferProgress is a no-op.
type XferProgress struct {
	verb       string // "push" or "pull"
	totalBytes int64
	reqObjs    atomic.Int64
	doneObjs   atomic.Int64
	doneBytes  atomic.Int64
	finished   atomic.Bool
}

// NewXferProgress returns a tracker for a transfer bounded by totalBytes.
func NewXferProgress(verb string, totalBytes int64) *XferProgress {
	return &XferProgress{verb: verb, totalBytes: totalBytes}
}

// Requested records one want round: objects the peer asked for (push) or
// this side is missing (pull). nil-safe.
func (x *XferProgress) Requested(objects int, bytes int64) {
	if x == nil {
		return
	}
	x.reqObjs.Add(int64(objects))
}

// Transferred records objects that moved, with exact payload bytes. nil-safe.
func (x *XferProgress) Transferred(objects int, bytes int64) {
	if x == nil {
		return
	}
	x.doneObjs.Add(int64(objects))
	x.doneBytes.Add(bytes)
}

// Finish marks the transfer complete so the bar renders full. nil-safe.
func (x *XferProgress) Finish() {
	if x == nil {
		return
	}
	x.finished.Store(true)
}

// render builds the progress line for the given elapsed time; pure, so
// the render loop and tests share it.
func (x *XferProgress) render(elapsed time.Duration) string {
	done := x.doneBytes.Load()
	objs := x.doneObjs.Load()
	req := x.reqObjs.Load()

	secs := elapsed.Seconds()
	var rate float64
	if secs > 0 {
		rate = float64(done) / secs
	}

	pct := 0.0
	switch {
	case x.finished.Load():
		pct = 100
	case x.totalBytes > 0:
		pct = float64(done) / float64(x.totalBytes) * 100
		if pct > 100 {
			pct = 100
		}
	}

	return fmt.Sprintf("%s %5.1f%% %s  %s/%s  %s/s  elapsed %s  objects %d/%d",
		x.verb,
		pct,
		bar(pct, 16),
		humanBytes(uint64(done)), humanBytes(uint64(x.totalBytes)),
		humanBytes(uint64(rate)),
		fmtDuration(elapsed),
		objs, req,
	)
}

// Run renders progress to w until ctx is cancelled; same contract as
// Progress.Run.
func (x *XferProgress) Run(ctx context.Context, w io.Writer, start time.Time, isTTY bool) {
	runProgressLoop(ctx, w, isTTY, x.render, start)
}

// summary is the one-line result printed after the transfer: what moved,
// or that nothing needed to.
func (x *XferProgress) summary(elapsed time.Duration) string {
	done := x.doneBytes.Load()
	objs := x.doneObjs.Load()
	if objs == 0 {
		return fmt.Sprintf("%s: everything up to date", x.verb)
	}
	secs := elapsed.Seconds()
	var rate float64
	if secs > 0 {
		rate = float64(done) / secs
	}
	return fmt.Sprintf("%sed %d objects, %s in %s (%s/s)",
		x.verb, objs, humanBytes(uint64(done)), fmtDuration(elapsed), humanBytes(uint64(rate)))
}
