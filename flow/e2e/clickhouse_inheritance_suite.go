package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
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

// inhRestartReplication pauses the mirror, ends its walsender and resumes it, so the next pull starts a new
// replication session in which pgoutput announces every relation again and the child map is rebuilt
func (s ClickHouseSuite) inhRestartReplication(env WorkflowRun, flowJobName string) {
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

func (s ClickHouseSuite) Test_Inheritance_Children_Created_After_Start() {
	s.inhRequirePostgres()

	srcTableName := "inh_late_children"
	parent := s.attachSchemaSuffix(srcTableName)
	dstTableName := "inh_late_children_dst"
	flowJobName := s.attachSuffix("inh_late_children")
	schemaX, schemaY := s.inhTenantSchema("lx"), s.inhTenantSchema("ly")

	// the parent has no children when CDC starts, so both children are found by runtime discovery; both
	// carry the child-only column note
	s.inhExec(`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT, val TEXT)`, parent)
	s.inhExec(`INSERT INTO %s (id, name, val) VALUES (1, 'p1', 'vp1')`, parent)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{parent: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,name,val")

	pub := connpostgres.GetDefaultPublicationName(flowJobName)
	s.inhExec(`CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, val TEXT, note TEXT, name TEXT)`, schemaX, srcTableName)
	s.inhExec(`ALTER TABLE %s.%s INHERIT %s`, schemaX, srcTableName, parent)
	s.inhExec(`CREATE TABLE %s.%s (note TEXT) INHERITS (%s)`, schemaY, srcTableName, parent)
	s.inhExec(`ALTER TABLE %s.%s ADD PRIMARY KEY (id)`, schemaY, srcTableName)
	s.inhExec(`ALTER PUBLICATION %s ADD TABLE %s.%s, %s.%s`, pub, schemaX, srcTableName, schemaY, srcTableName)

	s.inhExec(`INSERT INTO %s.%s (id, name, val, note) VALUES (2, 'x2', 'vx2', 'nx2')`, schemaX, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, note) VALUES (3, 'y3', 'vy3', 'ny3')`, schemaY, srcTableName)
	s.inhExec(`INSERT INTO %s.%s (id, name, val, note) VALUES (4, 'x4', 'vx4', 'nx4')`, schemaX, srcTableName)
	s.inhExec(`UPDATE %s.%s SET name = 'y3-upd' WHERE id = 3`, schemaY, srcTableName)
	s.inhExec(`UPDATE %s.%s SET val = 'vx2-upd' WHERE id = 2`, schemaX, srcTableName)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc from late children", srcTableName, dstTableName, "id,name,val")
	// the parent's own row gets the type's default, since the mirror is not in nullable mode
	s.inhWaitForColumn(env, dstTableName, "note", map[int64]string{1: "", 2: "nx2", 3: "ny3", 4: "nx4"})

	// both children report note as added, but the destination gets a single schema delta
	var noteDeltas int
	require.NoError(s.t, s.catalog.QueryRow(s.t.Context(),
		`SELECT count(*) FROM peerdb_stats.schema_deltas_audit_log l,
		 jsonb_array_elements(l.delta_info->'added_columns') AS c
		 WHERE l.flow_job_name = $1 AND c->>'name' = 'note'`, flowJobName).Scan(&noteDeltas))
	require.Equal(s.t, 1, noteDeltas)

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
