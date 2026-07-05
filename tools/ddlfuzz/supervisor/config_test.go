package main

import (
	"testing"
	"time"
)

func TestComputeDeadline(t *testing.T) {
	cutoff := jul6Cutoff()

	before := cutoff.Add(-24 * time.Hour)
	if got := computeDeadline(before, 72, false); !got.Equal(cutoff) {
		t.Errorf("before cutoff, no env: got %v, want %v", got, cutoff)
	}

	after := cutoff.Add(time.Hour)
	if got := computeDeadline(after, 72, false); !got.Equal(after.Add(72 * time.Hour)) {
		t.Errorf("after cutoff, no env: got %v, want %v", got, after.Add(72*time.Hour))
	}

	if got := computeDeadline(before, 10, true); !got.Equal(before.Add(10 * time.Hour)) {
		t.Errorf("env set before cutoff: got %v, want %v", got, before.Add(10*time.Hour))
	}
}
