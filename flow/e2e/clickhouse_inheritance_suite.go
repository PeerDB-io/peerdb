package e2e

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Tests for Postgres sources whose mirrored tables have inheritance children or partitions laid out
// differently from each other. pgoutput announces each relation's column layout before its first change, and
// again only after a schema change, so every child's tuples must be decoded with that child's own layout, not
// whichever sibling was announced last.

func (s ClickHouseSuite) inhRequirePostgres() {
	s.t.Helper()
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only applies to postgres")
	}
}

func (s ClickHouseSuite) inhExec(format string, args ...any) {
	s.t.Helper()
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(format, args...)))
}

// inhTenantSchema creates an extra schema, standing in for a tenant, dropped when the test ends
func (s ClickHouseSuite) inhTenantSchema(name string) string {
	s.t.Helper()
	schema := fmt.Sprintf("e2e_test_%s_%s", s.suffix, name)
	s.inhExec(`CREATE SCHEMA %s`, schema)
	s.t.Cleanup(func() {
		// the test's context is already canceled when cleanups run
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		pg := s.source.(*PostgresSource)
		if _, err := pg.PostgresConnector.Conn().Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)); err != nil {
			s.t.Logf("failed to drop tenant schema %s: %v", schema, err)
		}
	})
	return schema
}

// inhStopReplication pauses the mirror and waits until its walsender has ended
func (s ClickHouseSuite) inhStopReplication(env WorkflowRun, flowJobName string) {
	s.t.Helper()
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(s.t, env, 4*time.Minute, "pausing", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	conn := s.source.Connector().(*connpostgres.PostgresConnector).Conn()
	_, err := conn.Exec(s.t.Context(), fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE query LIKE '%%START_REPLICATION%%' AND query LIKE '%%%s%%' AND backend_type='walsender'`, flowJobName))
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "waiting for replication to stop", func() bool {
		rows, err := conn.Query(s.t.Context(), fmt.Sprintf(`SELECT pid FROM pg_stat_activity
			WHERE query LIKE '%%START_REPLICATION%%' AND query LIKE '%%%s%%' AND backend_type='walsender'`, flowJobName))
		require.NoError(s.t, err)
		defer rows.Close()
		return !rows.Next()
	})
}

// inhRestartReplication pauses the mirror, ends its walsender and resumes it, so the next pull starts a new
// replication session in which pgoutput announces every relation again and the child map is rebuilt
func (s ClickHouseSuite) inhRestartReplication(env WorkflowRun, flowJobName string) {
	s.t.Helper()
	s.inhStopReplication(env, flowJobName)
	SignalWorkflow(s.t.Context(), env, model.FlowSignal, model.NoopSignal)
	EnvWaitFor(s.t, env, 4*time.Minute, "resuming", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
}

// inhWaitForColumn waits until every non-deleted destination row has the expected value of column,
// keyed by id; ids missing from expected must be NULL
func (s ClickHouseSuite) inhWaitForColumn(env WorkflowRun, dstTable string, column string, expected map[int64]string) {
	s.t.Helper()
	EnvWaitFor(s.t, env, 3*time.Minute, "waiting on "+column, func() bool {
		rows, err := s.GetRows(dstTable, "id,"+column)
		if err != nil {
			s.t.Log(err)
			return false
		}
		matched := 0
		for _, row := range rows.Records {
			id, ok := row[0].Value().(int64)
			if !ok {
				s.t.Logf("unexpected id %v", row[0].Value())
				return false
			}
			want, hasWant := expected[id]
			got := row[1].Value()
			if !hasWant {
				if got != nil {
					s.t.Logf("id %d: expected NULL %s, got %v", id, column, got)
					return false
				}
				continue
			}
			if got != want {
				s.t.Logf("id %d: expected %s=%q, got %v", id, column, want, got)
				return false
			}
			matched++
		}
		return matched == len(expected)
	})
}

func (s ClickHouseSuite) Test_Inheritance_Divergent_Child_Layouts() {
	s.inhRequirePostgres()

	srcTableName := "inh_layouts"
	parent := s.attachSchemaSuffix(srcTableName)
	dstTableName := "inh_layouts_dst"
	flowJobName := s.attachSuffix("inh_layouts")
	schemaA, schemaB, schemaC, schemaD := s.inhTenantSchema("ta"), s.inhTenantSchema("tb"), s.inhTenantSchema("tc"), s.inhTenantSchema("td")

	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT, val TEXT, n INT, secret TEXT)`, parent)
	// A has the parent's column order
	s.inhExec(`CREATE TABLE %s.%s () INHERITS (%s)`, schemaA, srcTableName, parent)
	s.inhExec(`ALTER TABLE %s.%s ADD PRIMARY KEY (id)`, schemaA, srcTableName)
	// B was created on its own and INHERITed afterwards, so its text columns are swapped: decoding one with the
	// other's layout would silently exchange name and val
	s.inhExec(`CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, val TEXT, name TEXT, n INT, secret TEXT)`, schemaB, srcTableName)
	s.inhExec(`ALTER TABLE %s.%s INHERIT %s`, schemaB, srcTableName, parent)
	// C has a child-only column and replica identity full
	s.inhExec(`CREATE TABLE %s.%s (extra TEXT) INHERITS (%s)`, schemaC, srcTableName, parent)
	s.inhExec(`ALTER TABLE %s.%s ADD PRIMARY KEY (id)`, schemaC, srcTableName)
	s.inhExec(`ALTER TABLE %s.%s REPLICA IDENTITY FULL`, schemaC, srcTableName)

	s.inhExec(`INSERT INTO %s (id, name, val, n, secret) VALUES (1, 'p-name', 'p-val', 1, 's')`, parent)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n, secret) VALUES (2, 'a-name', 'a-val', 2, 's')`, schemaA, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n, secret) VALUES (3, 'b-name', 'b-val', 3, 's')`, schemaB, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n, secret, extra) VALUES (4, 'c-name', 'c-val', 4, 's', 'c-extra')`,
		schemaC, srcTableName)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: flowJobName,
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      parent,
			DestinationTableIdentifier: dstTableName,
			Exclude:                    []string{"secret"},
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	// few partitions over tiny tables: a partition spans the parent and a child
	flowConnConfig.SnapshotNumPartitionsOverride = 2
	flowConnConfig.Env = map[string]string{"PEERDB_NULLABLE": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,name,val,n")

	// CDC with the children interleaved, so each child's tuples follow another child's RelationMessage
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n) VALUES (11, 'a11', 'va11', 11)`, schemaA, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n) VALUES (12, 'b12', 'vb12', 12)`, schemaB, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n) VALUES (13, 'a13', 'va13', 13)`, schemaA, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n, extra) VALUES (14, 'c14', 'vc14', 14, 'c14-extra')`, schemaC, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n) VALUES (15, 'a15', 'va15', 15)`, schemaA, srcTableName)
	s.inhExec(`UPDATE %s.%s SET name = 'b12-upd' WHERE id = 12`, schemaB, srcTableName)
	s.inhExec(`DELETE FROM %s.%s WHERE id = 11`, schemaA, srcTableName)
	s.inhExec(`UPDATE %s.%s SET val = 'vc14-upd' WHERE id = 14`, schemaC, srcTableName)
	// a TOASTed value, then an update that leaves it unchanged. On a ReplacingMergeTree the value survives only
	// with replica identity full, where the old tuple (in C's own layout) fills in the unchanged column.
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n)
		VALUES (16, 'c16', (SELECT string_agg(md5(i::text), '') FROM generate_series(1, 3000) i), 16)`, schemaC, srcTableName)
	s.inhExec(`UPDATE %s.%s SET name = 'c16-upd' WHERE id = 16`, schemaC, srcTableName)
	s.inhExec(`INSERT INTO %s (id, name, val, n) VALUES (17, 'p17', 'vp17', 17)`, parent)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on interleaved cdc", srcTableName, dstTableName, "id,name,val,n")
	// the snapshot selects the parent's columns, so child-only values only arrive through CDC
	s.inhWaitForColumn(env, dstTableName, "extra", map[int64]string{14: "c14-extra"})
	s.inhExec(`UPDATE %s.%s SET extra = 'c4-extra-upd' WHERE id = 4`, schemaC, srcTableName)
	s.inhWaitForColumn(env, dstTableName, "extra", map[int64]string{4: "c4-extra-upd", 14: "c14-extra"})

	// a new replication session announces every relation again
	s.inhRestartReplication(env, flowJobName)

	// child DDL, and a child created after start that has to be added to the publication
	s.inhExec(`ALTER TABLE %s.%s ADD COLUMN extra2 TEXT`, schemaB, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n, extra2) VALUES (21, 'b21', 'vb21', 21, 'b21-extra2')`, schemaB, srcTableName)
	s.inhExec(`CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, n INT, val TEXT, name TEXT, secret TEXT)`, schemaD, srcTableName)
	s.inhExec(`ALTER TABLE %s.%s INHERIT %s`, schemaD, srcTableName, parent)
	s.inhExec(`ALTER PUBLICATION %s ADD TABLE %s.%s`, connpostgres.GetDefaultPublicationName(flowJobName), schemaD, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n) VALUES (22, 'd22', 'vd22', 22)`, schemaD, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, n) VALUES (23, 'a23', 'va23', 23)`, schemaA, srcTableName)
	s.inhExec(`UPDATE %s.%s SET val = 'vd22-upd' WHERE id = 22`, schemaD, srcTableName)
	s.inhExec(`UPDATE %s.%s SET name = 'b21-upd' WHERE id = 21`, schemaB, srcTableName)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc after restart", srcTableName, dstTableName, "id,name,val,n")
	s.inhWaitForColumn(env, dstTableName, "extra2", map[int64]string{21: "b21-extra2"})
	s.inhWaitForColumn(env, dstTableName, "extra", map[int64]string{4: "c4-extra-upd", 14: "c14-extra"})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Inheritance_Child_Of_Two_Mirrored_Parents() {
	s.inhRequirePostgres()

	parent1Name, parent2Name := "inh_multi_p1", "inh_multi_p2"
	parent1, parent2 := s.attachSchemaSuffix(parent1Name), s.attachSchemaSuffix(parent2Name)
	dst1, dst2 := "inh_multi_p1_dst", "inh_multi_p2_dst"
	flowJobName := s.attachSuffix("inh_multi")
	schemaM := s.inhTenantSchema("mm")

	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT)`, parent1)
	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT)`, parent2)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (1, 'p1')`, parent1)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (2, 'p2')`, parent2)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{parent1: dst1, parent2: dst2},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial p1", parent1Name, dst1, "id,name")
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial p2", parent2Name, dst2, "id,name")

	// created after start, so the first rows go through runtime discovery; inhseqno 1 is parent1
	s.inhExec(`CREATE TABLE %s.inh_multi_child (id BIGINT PRIMARY KEY, name TEXT) INHERITS (%s, %s)`,
		schemaM, parent1, parent2)
	s.inhExec(`ALTER PUBLICATION %s ADD TABLE %s.inh_multi_child`, connpostgres.GetDefaultPublicationName(flowJobName), schemaM)
	s.inhExec(`INSERT INTO %s.inh_multi_child (id, name) VALUES (10, 'm10')`, schemaM)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (3, 'p1-3')`, parent1)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (4, 'p2-4')`, parent2)

	// parent1's source rows include the child's, parent2's destination must only hold parent2's own rows
	EnvWaitForEqualTablesWithNames(env, s, "child routed to first parent", parent1Name, dst1, "id,name")
	EnvWaitForEqualTablesWithNames_Only(env, s, "child not routed to second parent", parent2Name, dst2, "id,name")

	// after a restart the child comes from the startup map instead, and must still go to parent1
	s.inhRestartReplication(env, flowJobName)
	s.inhExec(`INSERT INTO %s.inh_multi_child (id, name) VALUES (11, 'm11')`, schemaM)
	s.inhExec(`UPDATE %s.inh_multi_child SET name = 'm10-upd' WHERE id = 10`, schemaM)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (5, 'p2-5')`, parent2)

	EnvWaitForEqualTablesWithNames(env, s, "child still routed to first parent", parent1Name, dst1, "id,name")
	EnvWaitForEqualTablesWithNames_Only(env, s, "child still not routed to second parent", parent2Name, dst2, "id,name")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) inhPartitionDivergentLayout(pubViaRoot bool) {
	s.inhRequirePostgres()

	suffix := "nvr"
	if pubViaRoot {
		suffix = "vr"
	}
	srcTableName := "inh_parts_" + suffix
	root := s.attachSchemaSuffix(srcTableName)
	dstTableName := srcTableName + "_dst"
	flowJobName := s.attachSuffix("inh_parts_" + suffix)
	publication := s.attachSuffix("inh_parts_pub_" + suffix)

	s.inhExec(`CREATE TABLE %[1]s (id BIGINT NOT NULL, region TEXT NOT NULL, name TEXT, val TEXT,
		PRIMARY KEY (id, region)) PARTITION BY LIST (region)`, root)
	s.inhExec(`CREATE TABLE %[1]s_eu PARTITION OF %[1]s FOR VALUES IN ('eu')`, root)
	// attached from a table with its own column order
	s.inhExec(`CREATE TABLE %s_us (val TEXT, name TEXT, region TEXT NOT NULL, id BIGINT NOT NULL)`, root)
	s.inhExec(`ALTER TABLE %[1]s ATTACH PARTITION %[1]s_us FOR VALUES IN ('us')`, root)
	s.inhExec(`INSERT INTO %s (id, region, name, val) VALUES (1, 'eu', 'eu1', 'veu1'), (2, 'us', 'us2', 'vus2')`, root)
	// validation requires the root itself to be published, which only holds with publish_via_partition_root;
	// a mirror reaches leaf publishing when its publication is altered later, as done below
	s.inhExec(`CREATE PUBLICATION %s FOR TABLE %s WITH (publish_via_partition_root = true)`, publication, root)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{root: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.PublicationName = publication

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,region,name,val")

	if !pubViaRoot {
		s.inhExec(`ALTER PUBLICATION %s SET (publish_via_partition_root = false)`, publication)
		// the next pull reads the publication again and receives tuples under each partition's own relid
		s.inhRestartReplication(env, flowJobName)
	}

	s.inhExec(`INSERT INTO %s (id, region, name, val) VALUES (3, 'eu', 'eu3', 'veu3')`, root)
	s.inhExec(`INSERT INTO %s (id, region, name, val) VALUES (4, 'us', 'us4', 'vus4')`, root)
	s.inhExec(`INSERT INTO %s (id, region, name, val) VALUES (5, 'eu', 'eu5', 'veu5')`, root)
	s.inhExec(`UPDATE %s SET name = 'us2-upd' WHERE id = 2`, root)
	s.inhExec(`DELETE FROM %s WHERE id = 1`, root)
	s.inhExec(`INSERT INTO %s (id, region, name, val) VALUES (6, 'us', 'us6', 'vus6')`, root)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on interleaved cdc", srcTableName, dstTableName, "id,region,name,val")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Partition_Divergent_Layout_Publish_Via_Root() {
	s.inhPartitionDivergentLayout(true)
}

func (s ClickHouseSuite) Test_Partition_Divergent_Layout_Publish_Leaf() {
	s.inhPartitionDivergentLayout(false)
}

// inhChildSchemaMirror builds the PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN topology: a parent with two tenant
// children laid out differently, an ordinary table, and a partition root with a partition in a tenant schema.
// schemaL is for a child the ordinary table gains after the pull starts.
type inhChildSchemaMirror struct {
	parent, plain, parts                 string
	parentDst, plainDst, partsDst        string
	schemaA, schemaB, schemaP, ownSchema string
	schemaL                              string
	flowJobName                          string
}

func (s ClickHouseSuite) inhSetupChildSchemaMirror(name string) inhChildSchemaMirror {
	m := inhChildSchemaMirror{
		parent: name, plain: name + "_plain", parts: name + "_parts",
		parentDst: name + "_dst", plainDst: name + "_plain_dst", partsDst: name + "_parts_dst",
		schemaA: s.inhTenantSchema(name + "_a"), schemaB: s.inhTenantSchema(name + "_b"),
		schemaP:     s.inhTenantSchema(name + "_p"),
		schemaL:     s.inhTenantSchema(name + "_l"),
		ownSchema:   "e2e_test_" + s.suffix,
		flowJobName: s.attachSuffix(name),
	}
	parent := s.attachSchemaSuffix(m.parent)
	// the last two columns share their names with the schema map joined into generated snapshot queries
	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT, val TEXT, _peerdb_relid INT, _peerdb_nspname TEXT)`, parent)
	s.inhExec(`CREATE TABLE %s.%s () INHERITS (%s)`, m.schemaA, m.parent, parent)
	s.inhExec(`ALTER TABLE %s.%s ADD PRIMARY KEY (id)`, m.schemaA, m.parent)
	s.inhExec(`CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, val TEXT, _peerdb_nspname TEXT, name TEXT, _peerdb_relid INT)`,
		m.schemaB, m.parent)
	s.inhExec(`ALTER TABLE %s.%s INHERIT %s`, m.schemaB, m.parent, parent)
	s.inhExec(`INSERT INTO %s (id, name, val, _peerdb_relid, _peerdb_nspname) VALUES (1, 'p1', 'vp1', 1, 'n1')`, parent)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, _peerdb_relid, _peerdb_nspname) VALUES (2, 'a2', 'va2', 2, 'n2')`,
		m.schemaA, m.parent)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, _peerdb_relid, _peerdb_nspname) VALUES (3, 'b3', 'vb3', 3, 'n3')`,
		m.schemaB, m.parent)

	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT)`, s.attachSchemaSuffix(m.plain))
	s.inhExec(`INSERT INTO %s (id, name) VALUES (10, 'plain10')`, s.attachSchemaSuffix(m.plain))

	parts := s.attachSchemaSuffix(m.parts)
	s.inhExec(`CREATE TABLE %s (id BIGINT NOT NULL, region TEXT NOT NULL, name TEXT, PRIMARY KEY (id, region))
		PARTITION BY LIST (region)`, parts)
	s.inhExec(`CREATE TABLE %s.%s_eu PARTITION OF %s FOR VALUES IN ('eu')`, m.schemaP, m.parts, parts)
	s.inhExec(`INSERT INTO %s (id, region, name) VALUES (20, 'eu', 'eu20')`, parts)
	return m
}

func (m inhChildSchemaMirror) flowConfig(s ClickHouseSuite) *protos.FlowConnectionConfigs {
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: m.flowJobName,
		TableNameMapping: map[string]string{
			s.attachSchemaSuffix(m.parent): m.parentDst,
			s.attachSchemaSuffix(m.plain):  m.plainDst,
			s.attachSchemaSuffix(m.parts):  m.partsDst,
		},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	// nullable mode, so the map-named columns left NULL by later rows compare equal to the source
	flowConnConfig.Env = map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "true", "PEERDB_NULLABLE": "true"}
	return flowConnConfig
}

func (s ClickHouseSuite) inhWaitForTables(env WorkflowRun, m inhChildSchemaMirror, reason string) {
	s.t.Helper()
	EnvWaitForEqualTablesWithNames(env, s, reason+": parent", m.parent, m.parentDst, "id,name,val,_peerdb_relid,_peerdb_nspname")
	EnvWaitForEqualTablesWithNames(env, s, reason+": plain", m.plain, m.plainDst, "id,name")
	EnvWaitForEqualTablesWithNames(env, s, reason+": partitions", m.parts, m.partsDst, "id,region,name")
}

func (s ClickHouseSuite) inhChildSchemaAsColumn(name string, numPartitions uint32, extraEnv map[string]string) {
	s.inhRequirePostgres()
	m := s.inhSetupChildSchemaMirror(name)
	// the same key in two tenants: only the stamp tells the rows apart in the destination's sorting key, so both
	// must survive the snapshot, and changing one must leave the other alone
	s.inhExec(`INSERT INTO %s.%s (id, name, val) VALUES (30, 't30', 'v30')`, m.schemaA, m.parent)
	s.inhExec(`INSERT INTO %s.%s (id, name, val) VALUES (30, 't30', 'v30')`, m.schemaB, m.parent)
	flowConnConfig := m.flowConfig(s)
	// 1 makes a full-table snapshot and no override with CTID block partitioning off makes range partitions,
	// both reading the parent with each row's schema joined on tableoid; otherwise each table of the hierarchy
	// is read separately
	flowConnConfig.SnapshotNumPartitionsOverride = numPartitions
	maps.Copy(flowConnConfig.Env, extraEnv)

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	s.inhWaitForTables(env, m, "initial")
	// the snapshot read each row once: under FINAL a duplicate would collapse, so count the rows the snapshot synced
	EnvWaitFor(s.t, env, time.Minute, "snapshot row count", func() bool {
		var snapshotRows int64
		if err := s.catalog.QueryRow(s.t.Context(), `SELECT COALESCE(sum(qp.rows_synced), 0)
			FROM peerdb_stats.qrep_partitions qp JOIN peerdb_stats.qrep_runs qr
			ON qr.parent_mirror_name = qp.parent_mirror_name AND qr.run_uuid = qp.run_uuid
			WHERE qr.parent_mirror_name = $1 AND qr.destination_table = $2`,
			m.flowJobName, m.parentDst).Scan(&snapshotRows); err != nil {
			s.t.Log(err)
			return false
		}
		s.t.Logf("snapshot rows: %d", snapshotRows)
		return snapshotRows == 5
	})

	s.inhExec(`INSERT INTO %s.%s (id, name, val) VALUES (4, 'a4', 'va4')`, m.schemaA, m.parent)
	s.inhExec(`INSERT INTO %s.%s (id, name, val) VALUES (5, 'b5', 'vb5')`, m.schemaB, m.parent)
	s.inhExec(`UPDATE %s.%s SET name = 'b3-upd' WHERE id = 3`, m.schemaB, m.parent)
	s.inhExec(`DELETE FROM %s.%s WHERE id = 2`, m.schemaA, m.parent)
	s.inhExec(`DELETE FROM %s.%s WHERE id = 30`, m.schemaB, m.parent)
	s.inhExec(`UPDATE %s.%s SET name = 't30-upd' WHERE id = 30`, m.schemaA, m.parent)
	s.inhExec(`UPDATE %s SET name = 'p1-upd' WHERE id = 1`, s.attachSchemaSuffix(m.parent))
	s.inhExec(`INSERT INTO %s (id, name) VALUES (11, 'plain11')`, s.attachSchemaSuffix(m.plain))
	s.inhExec(`UPDATE %s SET name = 'plain10-upd' WHERE id = 10`, s.attachSchemaSuffix(m.plain))
	// the ordinary table had no children when the mirror started; a child it gains still stamps its own schema,
	// whether runtime discovery or a later pull's startup map routes it (TestRuntimeDiscoveredChildStampsOwnSchema
	// covers the former)
	s.inhExec(`CREATE TABLE %s.%s () INHERITS (%s)`, m.schemaL, m.plain, s.attachSchemaSuffix(m.plain))
	s.inhExec(`ALTER TABLE %s.%s ADD PRIMARY KEY (id)`, m.schemaL, m.plain)
	s.inhExec(`ALTER PUBLICATION %s ADD TABLE %s.%s`,
		connpostgres.GetDefaultPublicationName(m.flowJobName), m.schemaL, m.plain)
	s.inhExec(`INSERT INTO %s.%s (id, name) VALUES (12, 'late12')`, m.schemaL, m.plain)
	s.inhExec(`INSERT INTO %s (id, region, name) VALUES (21, 'eu', 'eu21')`, s.attachSchemaSuffix(m.parts))
	s.inhExec(`UPDATE %s SET name = 'eu20-upd' WHERE id = 20`, s.attachSchemaSuffix(m.parts))

	// equality under FINAL also proves updates collapsed onto the snapshot rows: _peerdb_source_schema leads the
	// sorting key, so a different stamp in CDC would leave two rows for one id
	s.inhWaitForTables(env, m, "cdc")
	s.inhWaitForColumn(env, m.parentDst, "_peerdb_source_schema", map[int64]string{
		1: m.ownSchema, 3: m.schemaB, 4: m.schemaA, 5: m.schemaB, 30: m.schemaA,
	})
	s.inhWaitForColumn(env, m.plainDst, "_peerdb_source_schema",
		map[int64]string{10: m.ownSchema, 11: m.ownSchema, 12: m.schemaL})
	// a declarative partition keeps the root's schema even when it lives elsewhere
	s.inhWaitForColumn(env, m.partsDst, "_peerdb_source_schema", map[int64]string{20: m.ownSchema, 21: m.ownSchema})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_Inheritance_Child_Schema_As_Column() {
	s.inhChildSchemaAsColumn("inh_cs2", 2, nil)
}

func (s ClickHouseSuite) Test_Inheritance_Child_Schema_As_Column_Full_Table_Snapshot() {
	s.inhChildSchemaAsColumn("inh_cs1", 1, nil)
}

func (s ClickHouseSuite) Test_Inheritance_Child_Schema_As_Column_Range_Snapshot() {
	s.inhChildSchemaAsColumn("inh_csr", 0, map[string]string{"PEERDB_POSTGRES_APPLY_CTID_BLOCK_PARTITIONING_OVERRIDE": "false"})
}

func (s ClickHouseSuite) Test_Inheritance_Child_Schema_As_Column_Old_Version() {
	s.inhRequirePostgres()
	m := s.inhSetupChildSchemaMirror("inh_cs_old")
	flowConnConfig := m.flowConfig(s)
	flowConnConfig.SnapshotNumPartitionsOverride = 2
	oldVersion := shared.InternalVersion_SourceSchemaFromInheritanceChild - 1
	flowConnConfig.Env["PEERDB_FORCE_INTERNAL_VERSION"] = strconv.FormatUint(uint64(oldVersion), 10)
	flowConnConfig.Version = oldVersion

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	s.inhWaitForTables(env, m, "initial")

	s.inhExec(`INSERT INTO %s.%s (id, name, val) VALUES (4, 'a4', 'va4')`, m.schemaA, m.parent)
	s.inhExec(`UPDATE %s.%s SET name = 'b3-upd' WHERE id = 3`, m.schemaB, m.parent)
	s.inhWaitForTables(env, m, "cdc")

	// mirrors created before the internal version keep stamping the mirrored table's schema
	s.inhWaitForColumn(env, m.parentDst, "_peerdb_source_schema", map[int64]string{
		1: m.ownSchema, 2: m.ownSchema, 3: m.ownSchema, 4: m.ownSchema,
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// A parent added to a running mirror stamps its children's schemas too, in its snapshot and in CDC.
func (s ClickHouseSuite) Test_Inheritance_Child_Schema_Table_Addition() {
	s.inhRequirePostgres()
	flowJobName := s.attachSuffix("inh_cs_add")
	plain, parent := s.attachSchemaSuffix("inh_cs_add_q"), s.attachSchemaSuffix("inh_cs_add_p")
	schema := s.inhTenantSchema("inh_cs_add_m")
	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT)`, plain)
	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT)`, parent)
	s.inhExec(`CREATE TABLE %s.inh_cs_add_p (id BIGINT PRIMARY KEY, name TEXT) INHERITS (%s)`, schema, parent)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (1, 'q1')`, plain)
	s.inhExec(`INSERT INTO %s (id, name) VALUES (2, 'p2')`, parent)
	s.inhExec(`INSERT INTO %s.inh_cs_add_p (id, name) VALUES (3, 'm3')`, schema)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{plain: "inh_cs_add_q_dst"},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial", "inh_cs_add_q", "inh_cs_add_q_dst", "id,name")

	s.inhStopReplication(env, flowJobName)
	SignalWorkflow(s.t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		AdditionalTables: []*protos.TableMapping{{
			SourceTableIdentifier:      parent,
			DestinationTableIdentifier: "inh_cs_add_p_dst",
			ShardingKey:                "id",
		}},
	})
	EnvWaitFor(s.t, env, 4*time.Minute, "adding the parent", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	s.inhExec(`INSERT INTO %s.inh_cs_add_p (id, name) VALUES (4, 'm4')`, schema)
	EnvWaitForEqualTablesWithNames(env, s, "parent added", "inh_cs_add_p", "inh_cs_add_p_dst", "id,name")
	s.inhWaitForColumn(env, "inh_cs_add_p_dst", "_peerdb_source_schema",
		map[int64]string{2: "e2e_test_" + s.suffix, 3: schema, 4: schema})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
