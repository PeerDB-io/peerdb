package corpus

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// firstByteFeature groups rows by the first SQL byte, which the tests below
// use as an explicit feature marker.
func firstByteFeature(_ uint8, _ uint64, sqlText []byte) uint64 {
	return uint64(sqlText[0])
}

func mkMarked(engine uint8, first, second byte, size int) run.Case {
	sqlBytes := bytes.Repeat([]byte{'x'}, size)
	sqlBytes[0] = first
	sqlBytes[1] = second
	return run.Case{SQL: sqlBytes, Engine: engine}
}

func mustAdd(t *testing.T, s *Store, c run.Case, signal uint8) run.Case {
	t.Helper()
	if ok, err := s.AddSignal(c, signal); err != nil || !ok {
		t.Fatalf("AddSignal(%q…, %s): ok=%v err=%v", c.SQL[:2], run.SignalName(signal), ok, err)
	}
	return c
}

func mustAddFeature(t *testing.T, s *Store, c run.Case, signal uint8, feature uint64) run.Case {
	t.Helper()
	featStr := strconv.FormatUint(feature, 10)
	if ok, err := s.AddSignalFeature(c, signal, featStr); err != nil || !ok {
		t.Fatalf("AddSignalFeature(%q…, %s, %s): ok=%v err=%v", c.SQL[:2], run.SignalName(signal), featStr, ok, err)
	}
	if got, ok := featureOf(t, s, c); !ok || got != featStr {
		t.Fatalf("AddSignalFeature stored feature %q (present=%v), want %q", got, ok, featStr)
	}
	return c
}

func featureOf(t *testing.T, s *Store, c run.Case) (string, bool) {
	t.Helper()
	var feature sql.NullString
	err := s.db.QueryRow(
		`SELECT feature FROM corpus WHERE engine = ? AND hash = ?`,
		run.EngineName(c.Engine), Hash(c.SQL, c.SQLMode),
	).Scan(&feature)
	if err != nil {
		t.Fatal(err)
	}
	return feature.String, feature.Valid
}

func survivorSizes(t *testing.T, s *Store, engine string) []int {
	t.Helper()
	cases, err := s.AllCases(engine)
	if err != nil {
		t.Fatal(err)
	}
	sizes := make([]int, 0, len(cases))
	for _, c := range cases {
		sizes = append(sizes, len(c.SQL))
	}
	slices.Sort(sizes)
	return sizes
}

func exists(t *testing.T, s *Store, c run.Case) bool {
	t.Helper()
	ok, err := s.existsLocked(run.EngineName(c.Engine), Hash(c.SQL, c.SQLMode))
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func setAddedAt(t *testing.T, s *Store, c run.Case, ts time.Time) {
	t.Helper()
	if _, err := s.db.Exec(
		`UPDATE corpus SET added_at = ? WHERE engine = ? AND hash = ?`,
		ts.UTC().Format(time.RFC3339), run.EngineName(c.Engine), Hash(c.SQL, c.SQLMode),
	); err != nil {
		t.Fatal(err)
	}
}

func TestDistillNoiseKeepsSmallestPerFeature(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "corpus.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	aLarge := mustAdd(t, s, mkMarked(run.EngineMySQL, 'A', 0, 40), run.SignalNoise)
	aSmall := mustAdd(t, s, mkMarked(run.EngineMySQL, 'A', 1, 10), run.SignalNoise)
	aTie := mustAdd(t, s, mkMarked(run.EngineMySQL, 'A', 2, 10), run.SignalNoise)
	bKeep := mustAdd(t, s, mkMarked(run.EngineMySQL, 'B', 0, 20), run.SignalNoise)
	mariaKeep := mustAdd(t, s, mkMarked(run.EngineMariaDB, 'A', 0, 30), run.SignalNoise)
	behavior := mustAdd(t, s, mkMarked(run.EngineMySQL, 'C', 0, 50), run.SignalBehavior)

	stats, err := s.DistillNoise(firstByteFeature)
	if err != nil {
		t.Fatal(err)
	}
	want := DistillStats{Scanned: 5, Kept: 3, Deleted: 2, Skipped: 0, BytesFreed: 50}
	if stats != want {
		t.Fatalf("DistillNoise stats = %+v, want %+v", stats, want)
	}

	if got, wantSizes := survivorSizes(t, s, "mysql"), []int{10, 20, 50}; !slices.Equal(got, wantSizes) {
		t.Fatalf("mysql survivors = %v, want %v", got, wantSizes)
	}
	if got, wantSizes := survivorSizes(t, s, "mariadb"), []int{30}; !slices.Equal(got, wantSizes) {
		t.Fatalf("mariadb survivors = %v, want %v", got, wantSizes)
	}
	if exists(t, s, aLarge) {
		t.Fatal("largest per-feature noise row survived")
	}
	if exists(t, s, aTie) {
		t.Fatal("tie must keep the lowest id; later duplicate survived")
	}
	for _, keeper := range []run.Case{aSmall, bKeep, mariaKeep} {
		feature, ok := featureOf(t, s, keeper)
		if !ok {
			t.Fatalf("keeper %q… not stamped with a feature", keeper.SQL[:2])
		}
		if wantFeat := strconv.FormatUint(uint64(keeper.SQL[0]), 10); feature != wantFeat {
			t.Fatalf("keeper feature = %q, want %q", feature, wantFeat)
		}
	}
	if _, ok := featureOf(t, s, behavior); ok {
		t.Fatal("behavior row was stamped; the behavior tier must stay untouched")
	}
	if err := s.Vacuum(); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
}

func TestDistillBehaviorWindowKeepsSmallestPerFeature(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "corpus.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	base := time.Date(2026, 7, 4, 10, 0, 0, 0, time.UTC)
	since, until := base.Add(time.Hour), base.Add(2*time.Hour)

	aLarge := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'A', 0, 40), run.SignalBehavior, 100)
	aSmall := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'A', 1, 10), run.SignalBehavior, 101)
	aTie := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'A', 2, 10), run.SignalBehavior, 102)
	bKeep := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'B', 0, 20), run.SignalBehavior, 103)
	outside := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'A', 3, 50), run.SignalBehavior, 104)
	noise := mustAdd(t, s, mkMarked(run.EngineMySQL, 'A', 4, 60), run.SignalNoise)
	mariaKeep := mustAddFeature(t, s, mkMarked(run.EngineMariaDB, 'A', 0, 30), run.SignalBehavior, 105)

	for _, c := range []run.Case{aLarge, aSmall, aTie, bKeep, mariaKeep} {
		setAddedAt(t, s, c, since)
	}
	setAddedAt(t, s, outside, until)
	setAddedAt(t, s, noise, since)

	stats, err := s.DistillBehaviorWindow(firstByteFeature, since, until)
	if err != nil {
		t.Fatal(err)
	}
	want := DistillStats{Scanned: 5, Kept: 3, Deleted: 2, Skipped: 0, BytesFreed: 50}
	if stats != want {
		t.Fatalf("DistillBehaviorWindow stats = %+v, want %+v", stats, want)
	}
	for _, gone := range []run.Case{aLarge, aTie} {
		if exists(t, s, gone) {
			t.Fatalf("window duplicate %q… survived", gone.SQL[:2])
		}
	}
	for _, kept := range []run.Case{aSmall, bKeep, outside, noise, mariaKeep} {
		if !exists(t, s, kept) {
			t.Fatalf("row %q… should have survived", kept.SQL[:2])
		}
	}
	for _, keeper := range []run.Case{aSmall, bKeep, mariaKeep} {
		feature, ok := featureOf(t, s, keeper)
		if !ok {
			t.Fatalf("keeper %q… not stamped with a feature", keeper.SQL[:2])
		}
		if wantFeat := strconv.FormatUint(uint64(keeper.SQL[0]), 10); feature != wantFeat {
			t.Fatalf("keeper feature = %q, want %q", feature, wantFeat)
		}
	}
	if got, _ := featureOf(t, s, outside); got != "104" {
		t.Fatalf("outside-window feature changed to %q", got)
	}
	if _, ok := featureOf(t, s, noise); ok {
		t.Fatal("noise row was stamped by behavior-window distill")
	}
}

func TestEvictOrderNoiseThenFeatureDuplicates(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "corpus.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	noiseBig := mustAdd(t, s, mkMarked(run.EngineMySQL, 'A', 0, 2000), run.SignalNoise)
	noiseSmall := mustAdd(t, s, mkMarked(run.EngineMySQL, 'B', 0, 1000), run.SignalNoise)
	keepF := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'D', 0, 3000), run.SignalBehavior, 7)
	dupF := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'E', 0, 5000), run.SignalBehavior, 7)
	soloG := mustAddFeature(t, s, mkMarked(run.EngineMySQL, 'F', 0, 4000), run.SignalBehavior, 8)

	s.mu.Lock()
	freedEnough, err := s.evictLocked(4000)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !freedEnough {
		t.Fatal("evictLocked(4000) = false, want true")
	}
	for _, gone := range []run.Case{noiseBig, noiseSmall, dupF} {
		if exists(t, s, gone) {
			t.Fatalf("row %q… should have been evicted", gone.SQL[:2])
		}
	}
	for _, kept := range []run.Case{keepF, soloG} {
		if !exists(t, s, kept) {
			t.Fatalf("row %q… should have survived eviction", kept.SQL[:2])
		}
	}
	if rows, freed := s.Evicted(); rows != 3 || freed != 8000 {
		t.Fatalf("Evicted() = (%d, %d), want (3, 8000)", rows, freed)
	}

	// Nothing evictable is left: only a feature keeper and a solo feature row.
	s.mu.Lock()
	freedEnough, err = s.evictLocked(1)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if freedEnough {
		t.Fatal("evictLocked(1) = true with no candidates left")
	}
	if got := s.Count("mysql"); got != 2 {
		t.Fatalf("Count after failed evict = %d, want 2", got)
	}
}

func TestAddEvictsOverBudget(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "corpus.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	noise := mustAdd(t, s, mkMarked(run.EngineMySQL, 'A', 0, 4096), run.SignalNoise)
	s.Budget = s.Bytes()

	added := mustAdd(t, s, mkMarked(run.EngineMySQL, 'B', 0, 512), run.SignalBehavior)
	if exists(t, s, noise) {
		t.Fatal("noise row survived an over-budget Add")
	}
	if !exists(t, s, added) {
		t.Fatal("over-budget Add did not insert after eviction")
	}
	if s.SkippedFull() != 0 {
		t.Fatalf("SkippedFull = %d, want 0 (eviction is not a skip)", s.SkippedFull())
	}
	if rows, freed := s.Evicted(); rows != 1 || freed != 4096 {
		t.Fatalf("Evicted() = (%d, %d), want (1, 4096)", rows, freed)
	}
	if s.freedCredit != 4096-512 {
		t.Fatalf("freedCredit = %d, want %d", s.freedCredit, 4096-512)
	}
}
