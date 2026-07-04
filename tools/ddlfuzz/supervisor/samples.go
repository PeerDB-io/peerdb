package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type SampleRecord struct {
	TS            time.Time      `json:"ts"`
	Fuzz          SampleFuzz     `json:"fuzz"`
	Findings      SampleFindings `json:"findings"`
	E2E           SampleE2E      `json:"e2e"`
	Queue         SampleQueue    `json:"queue"`
	Spend         TokenUsage     `json:"spend"`
	DiskFreeBytes int64          `json:"disk_free_bytes"`
}

type SampleFuzz struct {
	ExecsTotal int64            `json:"execs_total"`
	Suppressed int64            `json:"suppressed"`
	Corpus     map[string]int64 `json:"corpus"`
	Edges      map[string]int64 `json:"edges"`
	Restarts   map[string]int64 `json:"restarts"`
}

type SampleFindings struct {
	Open     int64 `json:"open"`
	Fixed    int64 `json:"fixed"`
	Ledgered int64 `json:"ledgered"`
	Parked   int64 `json:"parked"`
}

type SampleE2E struct {
	Cases       map[string]int64 `json:"cases"`
	ExecRejects map[string]int64 `json:"exec_rejects"`
}

type SampleQueue struct {
	Pending    int64 `json:"pending"`
	Processing int64 `json:"processing"`
	Done       int64 `json:"done"`
}

func CollectSample(cfg Config) SampleRecord {
	fast := readFastStats(cfg)
	counts, _ := findingsSummary(cfg)
	e2e := collectE2ECounters(readGenericJSON(filepath.Join(cfg.StateDir, "e2e-stats.json")))
	spend, _ := LoadSpend(cfg)
	free, _ := FreeBytes(cfg.StateDir)
	return SampleRecord{
		TS: time.Now().UTC(),
		Fuzz: SampleFuzz{
			ExecsTotal: fast.ExecsTotal,
			Suppressed: fast.Suppressed,
			Corpus:     copyInt64Map(fast.CorpusCount),
			Edges:      copyInt64Map(fast.Edges),
			Restarts:   copyInt64Map(fast.OracleRestarts),
		},
		Findings:      SampleFindings{Open: int64(counts.Open), Fixed: int64(counts.Fixed), Ledgered: int64(counts.Ledgered), Parked: int64(counts.Parked)},
		E2E:           SampleE2E{Cases: e2e.Cases, ExecRejects: e2e.ExecRejects},
		Queue:         queueDepths(cfg),
		Spend:         spend.Tokens,
		DiskFreeBytes: free,
	}
}

func AppendSample(cfg Config, rec SampleRecord) error {
	path := filepath.Join(cfg.StateDir, "samples.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func ReadSamplesTail(cfg Config, maxBytes int64) []SampleRecord {
	path := filepath.Join(cfg.StateDir, "samples.jsonl")
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return nil
	}
	if maxBytes <= 0 || maxBytes > info.Size() {
		maxBytes = info.Size()
	}
	start := info.Size() - maxBytes
	if _, err := f.Seek(start, 0); err != nil {
		return nil
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	if start > 0 && sc.Scan() {
	}
	var out []SampleRecord
	for sc.Scan() {
		var rec SampleRecord
		if json.Unmarshal(sc.Bytes(), &rec) == nil && !rec.TS.IsZero() {
			out = append(out, rec)
		}
	}
	return out
}

func Rate(samples []SampleRecord, extract func(SampleRecord) int64, window time.Duration, now time.Time) (float64, time.Duration, bool) {
	series := windowSamples(samples, window, now)
	if len(series) < 2 {
		return 0, 0, false
	}
	covered := series[len(series)-1].TS.Sub(series[0].TS)
	if covered <= 0 {
		return 0, 0, false
	}
	var sum int64
	prev := extract(series[0])
	for _, rec := range series[1:] {
		cur := extract(rec)
		if cur > prev {
			sum += cur - prev
		}
		prev = cur
	}
	return float64(sum) / covered.Seconds(), covered, true
}

func Sparkline(samples []SampleRecord, extract func(SampleRecord) int64, window time.Duration, buckets int) string {
	if buckets <= 0 {
		return ""
	}
	glyphs := []rune("▁▂▃▄▅▆▇█")
	out := make([]rune, buckets)
	for i := range out {
		out[i] = '·'
	}
	if len(samples) < 2 {
		return string(out)
	}
	now := samples[len(samples)-1].TS
	start := now.Add(-window)
	bucketDur := window / time.Duration(buckets)
	if bucketDur <= 0 {
		return string(out)
	}
	vals := make([]float64, buckets)
	prev := samples[0]
	for _, cur := range samples[1:] {
		if cur.TS.Before(start) {
			prev = cur
			continue
		}
		dt := cur.TS.Sub(prev.TS).Seconds()
		if dt > 0 {
			delta := extract(cur) - extract(prev)
			if delta > 0 {
				idx := int(cur.TS.Sub(start) / bucketDur)
				if idx >= buckets {
					idx = buckets - 1
				}
				if idx >= 0 {
					vals[idx] += float64(delta) / dt
				}
			}
		}
		prev = cur
	}
	var max float64
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	if max == 0 {
		return string(out)
	}
	for i, v := range vals {
		if v == 0 {
			continue
		}
		idx := int((v / max) * float64(len(glyphs)-1))
		out[i] = glyphs[idx]
	}
	return string(out)
}

func windowSamples(samples []SampleRecord, window time.Duration, now time.Time) []SampleRecord {
	cutoff := now.Add(-window)
	var out []SampleRecord
	for _, rec := range samples {
		if rec.TS.IsZero() || rec.TS.After(now) {
			continue
		}
		if !rec.TS.Before(cutoff) {
			out = append(out, rec)
		}
	}
	return out
}

func copyInt64Map(in map[string]int64) map[string]int64 {
	out := make(map[string]int64)
	for k, v := range in {
		out[k] = v
	}
	return out
}
