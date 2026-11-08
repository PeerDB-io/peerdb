package testhooks

import "fmt"

type CaseKind string

const (
	// CaseSingle arms one site.
	CaseSingle CaseKind = "single"
	// CaseRetryPair arms fail site First, then fail site Second once First has fired.
	CaseRetryPair CaseKind = "retry_pair"
	// CasePauseFail pauses one loop at First while the other loop fails at Second.
	CasePauseFail CaseKind = "pause_fail"
)

type Case struct {
	Second   *Site
	Kind     CaseKind
	First    Site
	Workload Workload
}

func (c Case) Name() string {
	if c.Second == nil {
		return fmt.Sprintf("%s/%s/%s", c.Workload, c.Kind, c.First.Name)
	}
	return fmt.Sprintf("%s/%s/%s+%s", c.Workload, c.Kind, c.First.Name, c.Second.Name)
}

// GenerateCases enumerates every case up to two events for each workload:
//   - every site alone;
//   - every ordered pair of distinct fail sites, the second firing after the first;
//   - every pause site combined with every fail site in the other loop.
func GenerateCases() []Case {
	var cases []Case
	for _, w := range Workloads {
		var fails, pauses []Site
		for _, s := range Sites {
			if !s.In(w) {
				continue
			}
			if s.Kind == KindPause {
				pauses = append(pauses, s)
			} else {
				fails = append(fails, s)
			}
		}

		for _, s := range Sites {
			if s.In(w) {
				cases = append(cases, Case{Workload: w, Kind: CaseSingle, First: s})
			}
		}
		for _, a := range fails {
			for _, b := range fails {
				if a.Name != b.Name {
					cases = append(cases, Case{Workload: w, Kind: CaseRetryPair, First: a, Second: &b})
				}
			}
		}
		for _, p := range pauses {
			for _, f := range fails {
				if otherLoop(p.Loop, f.Loop) {
					cases = append(cases, Case{Workload: w, Kind: CasePauseFail, First: p, Second: &f})
				}
			}
		}
	}
	return cases
}

func otherLoop(a, b Loop) bool {
	return (a == LoopSync && b == LoopNormalize) || (a == LoopNormalize && b == LoopSync)
}

// Shards is how many CI jobs run the generated cases. The workflow reads this
// constant to build its job matrix.
const Shards = 30

// ShardCases returns workload w's cases assigned to shard index of Shards.
// Sharding each workload separately gives every shard a similar mix.
func ShardCases(w Workload, index int) []Case {
	var cases []Case
	for _, c := range GenerateCases() {
		if c.Workload == w {
			cases = append(cases, c)
		}
	}
	return Shard(cases, index, Shards)
}

// Shard returns the cases assigned to shard index of total, round-robin.
// Cases are generated workload by workload, so round-robin spreads each
// workload, including the slow BigQuery one, evenly across shards.
func Shard(cases []Case, index, total int) []Case {
	var out []Case
	for i, c := range cases {
		if i%total == index {
			out = append(out, c)
		}
	}
	return out
}
