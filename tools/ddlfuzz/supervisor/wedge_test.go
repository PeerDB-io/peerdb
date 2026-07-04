package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatsStale(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	tests := []struct {
		name    string
		body    string
		field   string
		maxAge  time.Duration
		want    bool
		noWrite bool
	}{
		{
			name:   "RFC3339 string fresh",
			body:   fmt.Sprintf(`{"ts":%q}`, now.Format(time.RFC3339Nano)),
			field:  "ts",
			maxAge: time.Hour,
		},
		{
			name:   "RFC3339 string stale",
			body:   fmt.Sprintf(`{"ts":%q}`, now.Add(-2*time.Hour).Format(time.RFC3339Nano)),
			field:  "ts",
			maxAge: time.Hour,
			want:   true,
		},
		{
			name:   "unix int64 fresh",
			body:   fmt.Sprintf(`{"updated_at":%d}`, now.Unix()),
			field:  "updated_at",
			maxAge: time.Hour,
		},
		{
			name:   "unix int64 stale",
			body:   fmt.Sprintf(`{"updated_at":%d}`, now.Add(-2*time.Hour).Unix()),
			field:  "updated_at",
			maxAge: time.Hour,
			want:   true,
		},
		{
			name:   "float fraction stale",
			body:   fmt.Sprintf(`{"updated_at":%.1f}`, float64(now.Add(-2*time.Hour).Unix())+0.5),
			field:  "updated_at",
			maxAge: time.Hour,
			want:   true,
		},
		{
			name:   "missing field",
			body:   `{"updated_at":123}`,
			field:  "ts",
			maxAge: time.Hour,
		},
		{
			name:   "zero timestamp",
			body:   `{"updated_at":0}`,
			field:  "updated_at",
			maxAge: time.Hour,
		},
		{
			name:   "empty string",
			body:   `{"updated_at":""}`,
			field:  "updated_at",
			maxAge: time.Hour,
		},
		{
			name:   "invalid JSON",
			body:   `{"updated_at":`,
			field:  "updated_at",
			maxAge: time.Hour,
		},
		{
			name:    "missing file",
			field:   "updated_at",
			maxAge:  time.Hour,
			noWrite: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(tt.name, " ", "_")+".json")
			if !tt.noWrite {
				if err := os.WriteFile(path, []byte(tt.body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := statsStale(path, tt.field, tt.maxAge); got != tt.want {
				t.Fatalf("statsStale() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeFromAny(t *testing.T) {
	ts := time.Date(2026, 7, 4, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   any
		want time.Time
	}{
		{name: "RFC3339 string", in: ts.Format(time.RFC3339), want: ts},
		{name: "float64", in: float64(ts.Unix()), want: ts},
		{name: "json number", in: json.Number(fmt.Sprintf("%d", ts.Unix())), want: ts},
		{name: "int64", in: ts.Unix(), want: ts},
		{name: "int", in: int(ts.Unix()), want: ts},
		{name: "bool", in: true},
		{name: "nil", in: nil},
		{name: "negative", in: int64(-1)},
		{name: "zero", in: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := timeFromAny(tt.in)
			if !got.Equal(tt.want) {
				t.Fatalf("timeFromAny(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestEvalE2EWedge(t *testing.T) {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	procStart := now.Add(-10 * time.Minute)

	tests := []struct {
		name           string
		raw            map[string]any
		prevCases      map[string]int64
		procStart      time.Time
		wantWedged     bool
		reasonContains string
		wantNext       map[string]int64
	}{
		{
			name:      "fresh updated_at no case movement",
			raw:       e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(10, now.Add(-time.Minute).Unix())}),
			prevCases: map[string]int64{"mysql": 10},
			procStart: procStart,
			wantNext:  map[string]int64{"mysql": 10},
		},
		{
			name:           "stale numeric updated_at past grace",
			raw:            e2eRaw(now.Add(-2*time.Minute).Unix(), map[string]any{"mysql": engineRaw(10, now.Unix())}),
			prevCases:      map[string]int64{"mysql": 10},
			procStart:      procStart,
			wantWedged:     true,
			reasonContains: "stale stats",
			wantNext:       map[string]int64{"mysql": 10},
		},
		{
			name:      "stale updated_at within startup grace",
			raw:       e2eRaw(now.Add(-2*time.Minute).Unix(), map[string]any{"mysql": engineRaw(10, now.Unix())}),
			prevCases: map[string]int64{"mysql": 10},
			procStart: now.Add(-30 * time.Second),
			wantNext:  map[string]int64{"mysql": 10},
		},
		{
			name:           "stale RFC3339 updated_at",
			raw:            e2eRaw(now.Add(-2*time.Minute).Format(time.RFC3339), map[string]any{"mysql": engineRaw(10, now.Unix())}),
			prevCases:      map[string]int64{"mysql": 10},
			procStart:      procStart,
			wantWedged:     true,
			reasonContains: "stale stats",
			wantNext:       map[string]int64{"mysql": 10},
		},
		{
			name:      "cases advanced matcher fresh",
			raw:       e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(11, now.Add(-time.Minute).Unix())}),
			prevCases: map[string]int64{"mysql": 10},
			procStart: procStart,
			wantNext:  map[string]int64{"mysql": 11},
		},
		{
			name:           "cases advanced matcher lag",
			raw:            e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(11, now.Add(-10*time.Minute).Unix())}),
			prevCases:      map[string]int64{"mysql": 10},
			procStart:      procStart,
			wantWedged:     true,
			reasonContains: "matcher",
			wantNext:       map[string]int64{"mysql": 11},
		},
		{
			name:      "cases not advanced matcher lag",
			raw:       e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(10, now.Add(-10*time.Minute).Unix())}),
			prevCases: map[string]int64{"mysql": 10},
			procStart: procStart,
			wantNext:  map[string]int64{"mysql": 10},
		},
		{
			name:           "cases advanced zero matcher old procStart",
			raw:            e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(11, 0)}),
			prevCases:      map[string]int64{"mysql": 10},
			procStart:      procStart,
			wantWedged:     true,
			reasonContains: "matcher",
			wantNext:       map[string]int64{"mysql": 11},
		},
		{
			name:      "cases advanced zero matcher recent procStart",
			raw:       e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(11, 0)}),
			prevCases: map[string]int64{"mysql": 10},
			procStart: now.Add(-time.Minute),
			wantNext:  map[string]int64{"mysql": 11},
		},
		{
			name:      "first tick old matcher",
			raw:       e2eRaw(now.Unix(), map[string]any{"mysql": engineRaw(11, now.Add(-10*time.Minute).Unix())}),
			procStart: procStart,
			wantNext:  map[string]int64{"mysql": 11},
		},
		{
			name: "two engines one wedged",
			raw: e2eRaw(now.Unix(), map[string]any{
				"mysql":   engineRaw(11, now.Unix()),
				"mariadb": engineRaw(12, now.Add(-10*time.Minute).Unix()),
			}),
			prevCases:      map[string]int64{"mysql": 10, "mariadb": 11},
			procStart:      procStart,
			wantWedged:     true,
			reasonContains: "matcher",
			wantNext:       map[string]int64{"mysql": 11, "mariadb": 12},
		},
		{
			name:      "missing engines",
			raw:       map[string]any{"updated_at": float64(now.Unix())},
			procStart: procStart,
			wantNext:  map[string]int64{},
		},
		{
			name:      "engine value not map",
			raw:       e2eRaw(now.Unix(), map[string]any{"mysql": "bad"}),
			procStart: procStart,
			wantNext:  map[string]int64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, wedged, nextCases := evalE2EWedge(tt.raw, tt.prevCases, tt.procStart, now)
			if wedged != tt.wantWedged {
				t.Fatalf("wedged = %v, want %v; reason=%q", wedged, tt.wantWedged, reason)
			}
			if tt.reasonContains != "" && !strings.Contains(reason, tt.reasonContains) {
				t.Fatalf("reason = %q, want substring %q", reason, tt.reasonContains)
			}
			if !sameInt64Map(nextCases, tt.wantNext) {
				t.Fatalf("nextCases = %#v, want %#v", nextCases, tt.wantNext)
			}
		})
	}
}

func e2eRaw(updatedAt any, engines map[string]any) map[string]any {
	return map[string]any{
		"updated_at": updatedAt,
		"engines":    engines,
	}
}

func engineRaw(cases, matcherLastEventAt int64) map[string]any {
	return map[string]any{
		"cases":                 float64(cases),
		"matcher_last_event_at": float64(matcherLastEventAt),
	}
}

func sameInt64Map(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if b[k] != av {
			return false
		}
	}
	return true
}
