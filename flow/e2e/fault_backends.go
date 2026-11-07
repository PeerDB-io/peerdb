package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedTable
}

// PgFaultSource is a Postgres CDC source. It uses a pool, so the monitor and
// the test can use it concurrently.
type PgFaultSource struct {
	Pool   *pgxpool.Pool
	Schema string
}

func NewPgFaultSource(ctx context.Context, conn *pgx.Conn, schema string) (*PgFaultSource, error) {
	pool, err := pgxpool.New(ctx, conn.Config().ConnString())
	if err != nil {
		return nil, err
	}
	return &PgFaultSource{Pool: pool, Schema: schema}, nil
}

func (s *PgFaultSource) qualified(table string) string {
	return common.QuoteIdentifier(s.Schema) + "." + common.QuoteIdentifier(table)
}

func (s *PgFaultSource) CreateTable(ctx context.Context, table string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id BIGINT PRIMARY KEY, val TEXT NOT NULL)`, s.qualified(table)))
	return err
}

func (s *PgFaultSource) Insert(ctx context.Context, table string, row FaultRow, withExtra bool) error {
	if withExtra {
		_, err := s.Pool.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s (id, val, %s) VALUES ($1, $2, $3)`, s.qualified(table), FaultExtraColumn),
			row.ID, row.Val, row.Extra)
		return err
	}
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, val) VALUES ($1, $2)`, s.qualified(table)), row.ID, row.Val)
	return err
}

func (s *PgFaultSource) AddExtraColumn(ctx context.Context, table string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s TEXT`, s.qualified(table), FaultExtraColumn))
	return err
}

func (s *PgFaultSource) Durable(ctx context.Context, flowName string) (map[string]int64, error) {
	var lsn int64
	err := s.Pool.QueryRow(ctx,
		`SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1`,
		connpostgres.DefaultSlotPrefix+flowName,
	).Scan(&lsn)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return map[string]int64{"source.slot_confirmed_flush_lsn": lsn}, nil
}

// PgFaultDestination is a Postgres destination in Schema, with mirror
// metadata in MetadataSchema.
type PgFaultDestination struct {
	Pool           *pgxpool.Pool
	Schema         string
	MetadataSchema string
}

func NewPgFaultDestination(ctx context.Context, conn *pgx.Conn, schema string, peer *protos.Peer) (*PgFaultDestination, error) {
	pool, err := pgxpool.New(ctx, conn.Config().ConnString())
	if err != nil {
		return nil, err
	}
	metadataSchema := peer.GetPostgresConfig().GetMetadataSchema()
	if metadataSchema == "" {
		metadataSchema = "_peerdb_internal"
	}
	return &PgFaultDestination{Pool: pool, Schema: schema, MetadataSchema: metadataSchema}, nil
}

func (d *PgFaultDestination) qualified(table string) string {
	return common.QuoteIdentifier(d.Schema) + "." + common.QuoteIdentifier(table)
}

func (d *PgFaultDestination) Rows(ctx context.Context, table string) (map[int64]FaultRow, error) {
	hasExtra, err := d.HasExtraColumn(ctx, table)
	if err != nil {
		return nil, err
	}
	extra := "''"
	if hasExtra {
		extra = "coalesce(" + FaultExtraColumn + ", '')"
	}
	rows, err := d.Pool.Query(ctx, fmt.Sprintf(`SELECT id, val, %s FROM %s`, extra, d.qualified(table)))
	if isUndefinedTable(err) {
		return map[int64]FaultRow{}, nil
	} else if err != nil {
		return nil, err
	}
	out := map[int64]FaultRow{}
	for rows.Next() {
		var r FaultRow
		if err := rows.Scan(&r.ID, &r.Val, &r.Extra); err != nil {
			rows.Close()
			return nil, err
		}
		out[r.ID] = r
	}
	return out, rows.Err()
}

func (d *PgFaultDestination) HasExtraColumn(ctx context.Context, table string) (bool, error) {
	var ok bool
	err := d.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)`,
		d.Schema, table, FaultExtraColumn).Scan(&ok)
	return ok, err
}

func (d *PgFaultDestination) RunningNormalizes(ctx context.Context, table string) (int, error) {
	var n int
	err := d.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active'
		AND (query ILIKE 'MERGE INTO%' OR query ILIKE 'INSERT INTO%' OR query ILIKE 'WITH%')
		AND query ILIKE '%' || $1 || '%'`, table).Scan(&n)
	return n, err
}

// Throttle holds an EXCLUSIVE lock on the table: normalize writes block, reads
// (including the monitor's) do not.
func (d *PgFaultDestination) Throttle(ctx context.Context, table string, _ time.Duration) (func(), error) {
	conn, err := d.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s IN EXCLUSIVE MODE`, d.qualified(table))); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, err
	}
	return func() {
		_ = tx.Rollback(context.Background())
		conn.Release()
	}, nil
}

func (d *PgFaultDestination) AfterStart(ctx context.Context) error {
	return installStaleWriteTrigger(ctx, d.Pool, staleWriteTrigger{
		table:      common.QuoteIdentifier(d.MetadataSchema) + ".peerdb_mirror_jobs",
		name:       "peerdb_test_stale_mirror_jobs",
		key:        "mirror_job_name",
		insertCols: []string{"sync_batch_id"},
		updateCols: []string{"sync_batch_id", "normalize_batch_id"},
	})
}

func (d *PgFaultDestination) StaleWrites(ctx context.Context, flowName string) ([]string, error) {
	return staleWrites(ctx, d.Pool, flowName)
}

func (d *PgFaultDestination) Close() { d.Pool.Close() }

// ChFaultDestination is a ClickHouse destination.
type ChFaultDestination struct {
	Conn clickhouse.Conn
}

func NewChFaultDestination(ctx context.Context, peer *protos.Peer) (*ChFaultDestination, error) {
	conn, err := connclickhouse.Connect(ctx, nil, peer.GetClickhouseConfig())
	if err != nil {
		return nil, err
	}
	return &ChFaultDestination{Conn: conn}, nil
}

func (d *ChFaultDestination) Rows(ctx context.Context, table string) (map[int64]FaultRow, error) {
	hasExtra, err := d.HasExtraColumn(ctx, table)
	if err != nil {
		return nil, err
	}
	extra := "''"
	if hasExtra {
		extra = "ifNull(toString(" + FaultExtraColumn + "), '')"
	}
	rows, err := d.Conn.Query(ctx, fmt.Sprintf(
		"SELECT toInt64(id), toString(val), %s FROM `%s` FINAL WHERE _peerdb_is_deleted = 0 SETTINGS use_query_cache = false",
		extra, table))
	if err != nil {
		if strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			return map[int64]FaultRow{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	out := map[int64]FaultRow{}
	for rows.Next() {
		var r FaultRow
		if err := rows.Scan(&r.ID, &r.Val, &r.Extra); err != nil {
			return nil, err
		}
		out[r.ID] = r
	}
	return out, rows.Err()
}

func (d *ChFaultDestination) HasExtraColumn(ctx context.Context, table string) (bool, error) {
	var n uint64
	err := d.Conn.QueryRow(ctx,
		`SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?`,
		table, FaultExtraColumn).Scan(&n)
	return n > 0, err
}

func (d *ChFaultDestination) RunningNormalizes(ctx context.Context, table string) (int, error) {
	var n uint64
	err := d.Conn.QueryRow(ctx, `SELECT count() FROM system.processes
		WHERE is_initial_query AND query_kind = 'Insert' AND current_database = currentDatabase()
		AND position(query, ?) > 0`, "`"+table+"`").Scan(&n)
	return int(n), err
}

// Throttle attaches a materialized view that sleeps for slowness on every
// INSERT into table. Release drops the view without killing INSERTs already
// sleeping, so an old INSERT can outlive the throttle.
func (d *ChFaultDestination) Throttle(ctx context.Context, table string, slowness time.Duration) (func(), error) {
	mv := "`" + table + "_fault_throttle`"
	if err := d.Conn.Exec(ctx, fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s ENGINE = Null AS SELECT * FROM `%s` "+
			"WHERE 0 IN (SELECT number FROM numbers(%d) WHERE sleepEachRow(1) = 0 SETTINGS max_block_size = 1)",
		mv, table, int(slowness.Seconds()))); err != nil {
		return nil, err
	}
	return func() {
		_ = d.Conn.Exec(context.Background(), "DROP VIEW IF EXISTS "+mv)
	}, nil
}

func (d *ChFaultDestination) KillInserts(ctx context.Context, table string) error {
	return d.Conn.Exec(ctx, `KILL QUERY WHERE current_database = currentDatabase() AND query_kind = 'Insert'
		AND position(query, ?) > 0 ASYNC`, "`"+table+"`")
}

func (d *ChFaultDestination) StaleWrites(context.Context, string) ([]string, error) { return nil, nil }

func (d *ChFaultDestination) Close() { _ = d.Conn.Close() }

// CatalogCDCProgress reads CDC progress kept in the catalog, used by every
// destination except Postgres.
func CatalogCDCProgress(catalog *pgxpool.Pool) FaultProgressReader {
	return func(ctx context.Context, flowName string, tables []FaultTable) (FaultProgress, error) {
		var synced, normalized int64
		var watermarksJSON string
		err := catalog.QueryRow(ctx, `SELECT sync_batch_id, coalesce(normalize_batch_id, 0),
			coalesce(table_batch_id_data::text, '{}') FROM metadata_last_sync_state WHERE job_name = $1`,
			flowName).Scan(&synced, &normalized, &watermarksJSON)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return FaultProgress{}, err
		}
		watermarks := map[string]int64{}
		if err := json.Unmarshal([]byte(watermarksJSON), &watermarks); err != nil && watermarksJSON != "" {
			return FaultProgress{}, fmt.Errorf("table_batch_id_data: %w", err)
		}
		p := FaultProgress{
			Durable: map[string]int64{
				"catalog.sync_batch_id":      synced,
				"catalog.normalize_batch_id": normalized,
			},
			Batches:         map[string]BatchProgress{ProgressMirror: {Synced: synced, Normalized: normalized}},
			TableWatermarks: watermarks,
		}
		for table, id := range watermarks {
			p.Durable["catalog.table_batch_id."+table] = id
		}
		return p, nil
	}
}

// PgDestinationCDCProgress reads CDC progress kept in a Postgres destination.
func PgDestinationCDCProgress(d *PgFaultDestination) FaultProgressReader {
	return func(ctx context.Context, flowName string, tables []FaultTable) (FaultProgress, error) {
		var synced, normalized int64
		err := d.Pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT sync_batch_id, normalize_batch_id FROM %s.peerdb_mirror_jobs WHERE mirror_job_name = $1`,
			common.QuoteIdentifier(d.MetadataSchema)), flowName).Scan(&synced, &normalized)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) && !isUndefinedTable(err) {
			return FaultProgress{}, err
		}
		return FaultProgress{
			Durable: map[string]int64{
				"destination.sync_batch_id":      synced,
				"destination.normalize_batch_id": normalized,
			},
			Batches: map[string]BatchProgress{ProgressMirror: {Synced: synced, Normalized: normalized}},
		}, nil
	}
}

// QueryCDCProgress reads per-table query CDC progress and stage rows from the catalog.
func QueryCDCProgress(catalog *pgxpool.Pool) FaultProgressReader {
	return func(ctx context.Context, flowName string, tables []FaultTable) (FaultProgress, error) {
		p := FaultProgress{
			Durable:       map[string]int64{},
			Batches:       map[string]BatchProgress{},
			MissingStages: map[string][]int64{},
		}
		rows, err := catalog.Query(ctx, `SELECT source_table_identifier, synced_batch_id, normalized_batch_id
			FROM query_cdc_replication_state WHERE flow_name = $1`, flowName)
		if err != nil {
			return p, err
		}
		for rows.Next() {
			var table string
			var d BatchProgress
			if err := rows.Scan(&table, &d.Synced, &d.Normalized); err != nil {
				rows.Close()
				return p, err
			}
			p.Batches[table] = d
			p.Durable["query_cdc."+table+".synced_batch_id"] = d.Synced
			p.Durable["query_cdc."+table+".normalized_batch_id"] = d.Normalized
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return p, err
		}

		// Stages are read after progress: a stage deleted in between belongs to
		// a batch the destination read (later still) shows as applied.
		for table, d := range p.Batches {
			if d.Synced <= d.Normalized {
				continue
			}
			missing, err := catalog.Query(ctx, `SELECT b FROM generate_series($3::bigint + 1, $4::bigint) b
				WHERE NOT EXISTS (SELECT 1 FROM query_cdc_avro_stage s
					WHERE s.flow_name = $1 AND s.source_table_identifier = $2 AND s.batch_id = b)`,
				flowName, table, d.Normalized, d.Synced)
			if err != nil {
				return p, err
			}
			for missing.Next() {
				var b int64
				if err := missing.Scan(&b); err != nil {
					missing.Close()
					return p, err
				}
				p.MissingStages[table] = append(p.MissingStages[table], b)
			}
			missing.Close()
			if err := missing.Err(); err != nil {
				return p, err
			}
		}
		return p, nil
	}
}

// staleWriteTrigger records any write that proposes a lower progress value
// than the row holds. A BEFORE INSERT trigger sees the proposed row before
// ON CONFLICT runs, so it catches stale proposals that GREATEST would hide.
// UPDATE statements that clamp with GREATEST inside SET are not covered: the
// trigger only sees the clamped value. An INSERT proposing 0 is skipped:
// SetLastOffset inserts sync_batch_id=0 as a placeholder its ON CONFLICT
// branch never writes.
type staleWriteTrigger struct {
	table      string
	name       string
	key        string
	jsonCol    string
	insertCols []string
	updateCols []string
}

const staleWritesTable = "peerdb_test_stale_writes"

func (s staleWriteTrigger) functionSQL() string {
	var b strings.Builder
	logRow := func(col, oldExpr, newExpr string) {
		fmt.Fprintf(&b, "\t\tINSERT INTO %s (table_name, row_key, column_name, old_value, new_value) VALUES (%s, NEW.%s, %s, %s, %s);\n",
			staleWritesTable, quoteLiteral(s.table), s.key, col, oldExpr, newExpr)
	}
	compare := func(cols []string, skipZero bool) {
		for _, c := range cols {
			cond := fmt.Sprintf("NEW.%[1]s < cur.%[1]s", c)
			if skipZero {
				cond = fmt.Sprintf("NEW.%s > 0 AND %s", c, cond)
			}
			fmt.Fprintf(&b, "\tIF %s THEN\n", cond)
			logRow(quoteLiteral(c), "cur."+c, "NEW."+c)
			b.WriteString("\tEND IF;\n")
		}
	}
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $f$\n", s.name)
	fmt.Fprintf(&b, "DECLARE cur %s%%ROWTYPE; k text;\nBEGIN\n", s.table)
	fmt.Fprintf(&b, "IF TG_OP = 'INSERT' THEN\n\tSELECT * INTO cur FROM %s WHERE %s = NEW.%s;\n\tIF NOT FOUND THEN RETURN NEW; END IF;\n",
		s.table, s.key, s.key)
	compare(s.insertCols, true)
	b.WriteString("ELSE\n\tcur := OLD;\n")
	compare(s.updateCols, false)
	if s.jsonCol != "" {
		fmt.Fprintf(&b, "\tIF cur.%[1]s IS NOT NULL AND NEW.%[1]s IS NOT NULL THEN\n", s.jsonCol)
		fmt.Fprintf(&b, "\tFOR k IN SELECT jsonb_object_keys(cur.%s::jsonb) LOOP\n", s.jsonCol)
		fmt.Fprintf(&b, "\t\tIF (NEW.%[1]s::jsonb ->> k)::bigint < (cur.%[1]s::jsonb ->> k)::bigint THEN\n", s.jsonCol)
		logRow(quoteLiteral(s.jsonCol+".")+" || k",
			fmt.Sprintf("(cur.%s::jsonb ->> k)::bigint", s.jsonCol), fmt.Sprintf("(NEW.%s::jsonb ->> k)::bigint", s.jsonCol))
		b.WriteString("\t\tEND IF;\n\tEND LOOP;\n\tEND IF;\n")
	}
	b.WriteString("END IF;\nRETURN NEW;\nEND $f$")
	return b.String()
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func installStaleWriteTrigger(ctx context.Context, pool *pgxpool.Pool, s staleWriteTrigger) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// Parallel tests install the same trigger; serialize them.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7265300002)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+staleWritesTable+` (
			table_name TEXT NOT NULL, row_key TEXT NOT NULL, column_name TEXT NOT NULL,
			old_value BIGINT, new_value BIGINT, at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, s.functionSQL()); err != nil {
			return fmt.Errorf("stale write function for %s: %w", s.table, err)
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = $1)`, s.name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %[1]s BEFORE INSERT OR UPDATE ON %[2]s
			FOR EACH ROW EXECUTE FUNCTION %[1]s()`, s.name, s.table))
		return err
	})
}

func installCatalogStaleWriteTriggers(ctx context.Context, catalog *pgxpool.Pool) error {
	return installStaleWriteTrigger(ctx, catalog, staleWriteTrigger{
		table:      "metadata_last_sync_state",
		name:       "peerdb_test_stale_last_sync_state",
		key:        "job_name",
		jsonCol:    "table_batch_id_data",
		insertCols: []string{"sync_batch_id"},
		updateCols: []string{"sync_batch_id", "normalize_batch_id"},
	})
}

func staleWrites(ctx context.Context, pool *pgxpool.Pool, flowName string) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT table_name, column_name, old_value, new_value FROM `+staleWritesTable+`
		WHERE row_key = $1 ORDER BY at`, flowName)
	if isUndefinedTable(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var table, col string
		var oldValue, newValue int64
		if err := rows.Scan(&table, &col, &oldValue, &newValue); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%s.%s %d -> %d", table, col, oldValue, newValue))
	}
	return out, rows.Err()
}

func catalogStaleWrites(ctx context.Context, catalog *pgxpool.Pool, flowName string) ([]string, error) {
	return staleWrites(ctx, catalog, flowName)
}
