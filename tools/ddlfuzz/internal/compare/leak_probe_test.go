package compare

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/oracle"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// Scratch diagnostic for the live retention flood: recompute BehaviorFeatures
// for recently retained rows with the real oracle digest in hand, ablate each
// tuple field, and show what maskedError leaves of reject errors.
func TestLeakProbe(t *testing.T) {
	stateDB := os.Getenv("LEAK_PROBE")
	if stateDB == "" {
		t.Skip("set LEAK_PROBE=<path to corpus.db>")
	}
	db, err := sql.Open("sqlite", "file:"+stateDB+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type row struct {
		c run.Case
	}
	var rows []row
	res, err := db.Query(`SELECT sql, sql_mode, engine FROM corpus WHERE signal='behavior' ORDER BY id DESC LIMIT 3000`)
	if err != nil {
		t.Fatal(err)
	}
	for res.Next() {
		var sqlText []byte
		var mode, engine string
		if err := res.Scan(&sqlText, &mode, &engine); err != nil {
			t.Fatal(err)
		}
		m, _ := strconv.ParseUint(mode, 10, 64)
		e := run.EngineMySQL
		if engine == "mariadb" {
			e = run.EngineMariaDB
		}
		rows = append(rows, row{run.Case{SQL: sqlText, SQLMode: m, Engine: e}})
	}
	res.Close()
	t.Logf("sampled %d rows", len(rows))

	byEngine := map[uint8][]run.Case{}
	for _, r := range rows {
		byEngine[r.c.Engine] = append(byEngine[r.c.Engine], r.c)
	}

	worker := ddllexec.NewWorker(0, 2*time.Second, nil)
	ctx := context.Background()
	root := "../.."

	distinct := map[string]map[uint64]struct{}{}
	count := func(key string, v uint64) {
		if distinct[key] == nil {
			distinct[key] = map[uint64]struct{}{}
		}
		distinct[key][v] = struct{}{}
	}
	verdicts := map[string]int{}
	printed := 0

	for eng, cases := range byEngine {
		name := run.EngineName(eng)
		client := oracle.NewClient(name, root+"/build/oracle-"+name, 30*time.Second)
		if err := client.Start(ctx, os.DevNull); err != nil {
			t.Fatalf("oracle %s: %v", name, err)
		}
		for start := 0; start < len(cases); start += 500 {
			batch := cases[start:min(start+500, len(cases))]
			digests, _, _, err := client.ParseBatch(ctx, batch)
			if err != nil {
				t.Fatalf("ParseBatch %s: %v", name, err)
			}
			results := worker.RunBatch(batch)
			for i, c := range batch {
				d := digests[i]
				r := results[i]
				verdicts[name+"/"+d.Verdict]++
				full := BehaviorFeature(c, r.Sig, r.Err, r.Panic, d)
				count("full", full)
				count("ourside-only(d=nil)", BehaviorFeature(c, r.Sig, r.Err, r.Panic, nil))
				count("no-oracle-err(verdict-only)", BehaviorFeature(c, r.Sig, r.Err, r.Panic, &digest.Digest{Verdict: d.Verdict, Stmts: d.Stmts}))
				if d.Verdict == "reject" && printed < 15 {
					printed++
					fmt.Printf("RAW : %.110q\nNORM: %.110q\n---\n", d.Error, NormalizeError(d.Error))
				}
			}
		}
		_ = client.Close()
	}
	t.Logf("verdicts: %v", verdicts)
	for k, s := range distinct {
		t.Logf("distinct %-28s = %d / %d", k, len(s), len(rows))
	}
}
