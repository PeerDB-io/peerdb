package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	e2eStatsStaleAfter = 60 * time.Second
	e2eMatcherLagMax   = 300 * time.Second
	e2eStartupGrace    = 90 * time.Second
)

func timeFromAny(v any) time.Time {
	if s, ok := v.(string); ok {
		return parseLooseTime(s)
	}
	switch v.(type) {
	case float64, int64, int, json.Number:
		n := int64FromAny(v)
		if n > 0 {
			return time.Unix(n, 0)
		}
	}
	return time.Time{}
}

func statsStale(path, tsField string, maxAge time.Duration) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var obj map[string]any
	if jsonErr := jsonUnmarshal(data, &obj); jsonErr != nil {
		return false
	}
	t := timeFromAny(obj[tsField])
	return !t.IsZero() && time.Since(t) > maxAge
}

type e2eWedgeCheck struct {
	procStart time.Time
	prevCases map[string]int64
}

func newE2EWedgeCheck(procStart time.Time) *e2eWedgeCheck {
	return &e2eWedgeCheck{procStart: procStart}
}

func (w *e2eWedgeCheck) check(path string, now time.Time) (reason string, wedged bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var raw map[string]any
	if jsonErr := jsonUnmarshal(data, &raw); jsonErr != nil {
		return "", false
	}
	reason, wedged, nextCases := evalE2EWedge(raw, w.prevCases, w.procStart, now)
	w.prevCases = nextCases
	return reason, wedged
}

func evalE2EWedge(raw map[string]any, prevCases map[string]int64, procStart, now time.Time) (reason string, wedged bool, nextCases map[string]int64) {
	nextCases = make(map[string]int64)
	engines, _ := raw["engines"].(map[string]any)
	for engine, v := range engines {
		stats, ok := v.(map[string]any)
		if !ok {
			continue
		}
		nextCases[engine] = int64FromAny(stats["cases"])
	}

	ts := timeFromAny(raw["updated_at"])
	// The stats file on disk can belong to the previous lane for the first tick after restart.
	if !ts.IsZero() && now.Sub(ts) > e2eStatsStaleAfter && now.Sub(procStart) > e2eStartupGrace {
		return fmt.Sprintf("stale stats updated_at age %s", now.Sub(ts).Round(time.Second)), true, nextCases
	}

	for engine, v := range engines {
		stats, ok := v.(map[string]any)
		if !ok {
			continue
		}
		cases := int64FromAny(stats["cases"])
		prev, hadPrev := prevCases[engine]
		if !hadPrev || cases <= prev {
			continue
		}
		lastEventAt := int64FromAny(stats["matcher_last_event_at"])
		last := timeFromAny(lastEventAt)
		if last.IsZero() {
			last = procStart
		}
		if now.Sub(last) > e2eMatcherLagMax {
			return fmt.Sprintf("matcher lag for %s age %s while cases advanced", engine, now.Sub(last).Round(time.Second)), true, nextCases
		}
	}
	return "", false, nextCases
}

func jsonUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
