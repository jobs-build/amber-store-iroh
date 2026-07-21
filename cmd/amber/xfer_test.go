package main

import (
	"strings"
	"testing"
	"time"
)

func TestXferRender(t *testing.T) {
	x := NewXferProgress("push", 200)
	x.Requested(10, 220)
	x.Transferred(3, 50)
	x.Wire(30)
	line := x.render(2 * time.Second)
	for _, want := range []string{"push", " 25.0% ", "50 B/200 B", "25 B/s", "(wire 15 B/s)", "elapsed 0:02", "objects 3/10"} {
		if !strings.Contains(line, want) {
			t.Fatalf("render missing %q: %s", want, line)
		}
	}
}

func TestXferRenderClampsAndFinishes(t *testing.T) {
	x := NewXferProgress("pull", 100)
	x.Transferred(1, 250) // logical total is an upper bound, but never exceed 100%
	if line := x.render(time.Second); !strings.Contains(line, "100.0%") {
		t.Fatalf("overshoot must clamp: %s", line)
	}

	// Dedup can end a transfer far below the footprint; Finish fills the bar.
	y := NewXferProgress("push", 1<<20)
	y.Transferred(1, 10)
	if line := y.render(time.Second); strings.Contains(line, "100.0%") {
		t.Fatalf("unfinished transfer must not show full: %s", line)
	}
	y.Finish()
	if line := y.render(time.Second); !strings.Contains(line, "100.0%") {
		t.Fatalf("finished transfer must render full: %s", line)
	}
}

func TestXferSummary(t *testing.T) {
	x := NewXferProgress("push", 1000)
	if got := x.summary(time.Second); got != "push: everything up to date" {
		t.Fatalf("empty transfer summary: %q", got)
	}
	x.Transferred(12, 2048)
	x.Wire(1024)
	got := x.summary(2 * time.Second)
	for _, want := range []string{"pushed 12 objects", "2.0 KiB", "0:02", "1.0 KiB/s", "wire 1.0 KiB", "512 B/s"} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q: %q", want, got)
		}
	}
}

func TestXferNilSafe(t *testing.T) {
	var x *XferProgress
	x.Requested(1, 1)
	x.Transferred(1, 1)
	x.Wire(1)
	x.Finish()
}
