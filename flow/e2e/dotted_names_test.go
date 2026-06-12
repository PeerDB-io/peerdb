package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// dottedDestinationTable returns a destination identifier for a dotted-name test,
// dotted wherever the destination system permits dots (see the dot-capability matrix
// in the qualified-table-identifiers plan): ClickHouse allows dots in its single-part
// table names, Postgres and Snowflake allow dotted schema and table, BigQuery allows
// neither so it gets a dotless name (still exercising the dotted source).
func dottedDestinationTable(s GenericSuite, srcSchema string, table string) common.QualifiedTable {
	switch s.(type) {
	case PeerFlowE2ETestSuitePG:
		return common.QualifiedTable{Namespace: srcSchema, Table: table}
	case PeerFlowE2ETestSuiteSF:
		return common.QualifiedTable{Namespace: "E2E.DOT_" + strings.ToUpper(s.Suffix()), Table: strings.ToUpper(table)}
	case PeerFlowE2ETestSuiteBQ:
		return common.QualifiedTable{Table: strings.ReplaceAll(table, ".", "_")}
	case ClickHouseSuite:
		return common.QualifiedTable{Table: table}
	default:
		panic(fmt.Sprintf("unhandled suite type %T for dotted destination", s))
	}
}

func setupDottedDestination(t *testing.T, s GenericSuite, dst common.QualifiedTable) {
	t.Helper()
	// Snowflake's SetupNormalizedTable does not create missing schemas; the Postgres
	// destination reuses the already-created dotted source schema (same database)
	if sf, ok := s.(PeerFlowE2ETestSuiteSF); ok {
		require.NoError(t, sf.sfHelper.RunCommand(t.Context(), fmt.Sprintf(
			`CREATE TRANSIENT SCHEMA IF NOT EXISTS %s."%s"`, sf.sfHelper.testDatabaseName, dst.Namespace)))
	}
}

// dottedDestinationRows fetches destination rows with quoting-safe SQL; the stock
// suite GetRows helpers interpolate an unquoted dotless schema and so cannot read
// dotted destination namespaces.
func dottedDestinationRows(s GenericSuite, dst common.QualifiedTable, cols string) (*model.QRecordBatch, error) {
	t := s.T()
	switch v := s.(type) {
	case PeerFlowE2ETestSuitePG:
		executor, err := v.conn.NewQRepQueryExecutor(t.Context(), nil, shared.InternalVersion_Latest, "testflow", "testpart")
		if err != nil {
			return nil, err
		}
		return executor.ExecuteAndProcessQuery(t.Context(), fmt.Sprintf(
			`SELECT %s FROM %s.%s ORDER BY id`,
			cols, common.QuoteIdentifier(dst.Namespace), common.QuoteIdentifier(dst.Table)))
	case PeerFlowE2ETestSuiteSF:
		return v.sfHelper.ExecuteAndProcessQuery(t.Context(), fmt.Sprintf(
			`SELECT %s FROM %s."%s"."%s" ORDER BY id`,
			cols, v.sfHelper.testDatabaseName, dst.Namespace, dst.Table))
	case PeerFlowE2ETestSuiteBQ, ClickHouseSuite:
		// both quote the table identifier they receive, so dots are safe
		return s.GetRows(dst.Table, cols)
	default:
		return nil, fmt.Errorf("unhandled suite type %T for dotted destination", s)
	}
}

func envWaitForEqualDottedTables(
	env WorkflowRun,
	s Generic,
	reason string,
	source *PostgresSource,
	srcQuoted string,
	dst common.QualifiedTable,
	cols string,
) {
	t := s.T()
	t.Helper()

	EnvWaitFor(t, env, 3*time.Minute, reason, func() bool {
		t.Helper()

		srcRows, err := source.Query(t.Context(), fmt.Sprintf(`SELECT %s FROM %s ORDER BY id`, cols, srcQuoted))
		if err != nil {
			t.Log(err)
			return false
		}

		dstRows, err := dottedDestinationRows(s.GenericSuite, dst, cols)
		if err != nil {
			t.Log(err)
			return false
		}

		return e2eshared.CheckEqualRecordBatches(t, srcRows, dstRows)
	})
}

func (s Generic) Test_Dotted_Names_CDC() {
	t := s.T()

	source, ok := s.Source().(*PostgresSource)
	if !ok {
		t.Skip("dotted source identifiers are only legal on Postgres sources")
	}

	srcSchema := "e2e_dot." + s.Suffix()
	srcTable := "ta.ble"
	srcQuoted := common.QuoteIdentifier(srcSchema) + "." + common.QuoteIdentifier(srcTable)
	dst := dottedDestinationTable(s.GenericSuite, srcSchema, "ta.ble_dst")

	require.NoError(t, s.Source().Exec(t.Context(), "CREATE SCHEMA "+common.QuoteIdentifier(srcSchema)))
	t.Cleanup(func() {
		// the suite teardown only drops the dotless e2e_test_<suffix> schema
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = s.Source().Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", common.QuoteIdentifier(srcSchema)))
	})
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id SERIAL PRIMARY KEY, val TEXT NOT NULL)", srcQuoted)))
	setupDottedDestination(t, s.GenericSuite, dst)

	for i := range 5 {
		require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(
			"INSERT INTO %s (val) VALUES ('init_%d')", srcQuoted, i)))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: AddSuffix(s, "dotted_cdc"),
		TableMappings: []*protos.TableMapping{{
			SourceTable:      &protos.QualifiedTable{Namespace: srcSchema, Table: srcTable},
			DestinationTable: internal.QualifiedTableProto(dst),
			ShardingKey:      "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	envWaitForEqualDottedTables(env, s, "initial snapshot", source, srcQuoted, dst, "id,val")

	for i := range 5 {
		EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
			"INSERT INTO %s (val) VALUES ('cdc_%d')", srcQuoted, i)))
	}
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
		"UPDATE %s SET val=val||'_updated' WHERE id%%2=0", srcQuoted)))
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
		"DELETE FROM %s WHERE id=1", srcQuoted)))

	envWaitForEqualDottedTables(env, s, "cdc insert/update/delete", source, srcQuoted, dst, "id,val")

	// schema change on a dotted table: the delta carries QualifiedTable structs and the
	// destination ALTER must quote per component
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN val2 TEXT", srcQuoted)))
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
		"INSERT INTO %s (val, val2) VALUES ('post_alter', 'v2')", srcQuoted)))

	envWaitForEqualDottedTables(env, s, "schema change on dotted table", source, srcQuoted, dst, "id,val,val2")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s APITestSuite) TestDottedTableAddition() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("dotted source identifiers are only legal on Postgres sources")
	}

	baseTable := "dotadd_base"
	dottedTable := "dot.added"
	dottedDst := "dot.added_dst"
	dottedSrcQuoted := common.QuoteIdentifier(Schema(s)) + "." + common.QuoteIdentifier(dottedTable)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, baseTable))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", dottedSrcQuoted)))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) VALUES (1,'base')", AttachSchema(s, baseTable))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) VALUES (1,'snapshot')", dottedSrcQuoted)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "dotted_add_table_" + s.suffix,
		TableMappings: []*protos.TableMapping{{
			SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: baseTable},
			DestinationTable: &protos.QualifiedTable{Table: baseTable},
			ShardingKey:      "id",
		}},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, baseTable, "id,val")

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{
						{
							SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: dottedTable},
							DestinationTable: &protos.QualifiedTable{Table: dottedDst},
						},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table addition to finish", func() bool {
		valid, err := s.checkCatalogTableMapping(s.t.Context(), flowConnConfig.FlowJobName, []common.QualifiedTable{
			{Namespace: Schema(s), Table: baseTable},
			{Namespace: Schema(s), Table: dottedTable},
		})
		if err != nil {
			return false
		}

		return valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	EnvWaitForEqualTablesWithNames(env, s.ch, "added dotted table initial load", dottedTable, dottedDst, "id,val")

	EnvNoError(s.t, env, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) VALUES (2,'cdc')", dottedSrcQuoted)))
	EnvWaitForEqualTablesWithNames(env, s.ch, "added dotted table cdc", dottedTable, dottedDst, "id,val")

	// additions must also be validated against the flow's EXISTING tables
	for _, tc := range []struct {
		name        string
		mapping     *protos.TableMapping
		errContains string
	}{
		{
			name: "duplicate of existing destination",
			mapping: &protos.TableMapping{
				SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "dotadd_other"},
				DestinationTable: &protos.QualifiedTable{Table: baseTable},
			},
			errContains: "duplicate destination table",
		},
		{
			name: "dotted collision with existing destination",
			mapping: &protos.TableMapping{
				SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "dotadd_other"},
				DestinationTable: &protos.QualifiedTable{Namespace: "dot", Table: "added_dst"},
			},
			errContains: "ambiguous",
		},
	} {
		s.t.Run(tc.name, func(t *testing.T) {
			_, err := s.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
				FlowJobName:        flowConnConfig.FlowJobName,
				RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
				FlowConfigUpdate: &protos.FlowConfigUpdate{
					Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
						CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
							AdditionalTables: []*protos.TableMapping{tc.mapping},
						},
					},
				},
			})
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok, "expected gRPC status error")
			require.Equal(t, codes.InvalidArgument, st.Code())
			require.Contains(t, st.Message(), "conflict with existing tables")
			require.Contains(t, st.Message(), tc.errContains)
		})
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestMirrorValidation_DottedIdentifierCollisions() {
	sourcePeerName := s.source.GeneratePeer(s.t).Name
	destinationPeerName := s.ch.Peer().Name

	tests := []struct {
		name        string
		mappings    []*protos.TableMapping
		errContains string
	}{
		{
			name: "duplicate destination structs",
			mappings: []*protos.TableMapping{
				{
					SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc1"},
					DestinationTable: &protos.QualifiedTable{Table: "vdst"},
				},
				{
					SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc2"},
					DestinationTable: &protos.QualifiedTable{Table: "vdst"},
				},
			},
			errContains: "duplicate destination table",
		},
		{
			name: "legacy dotted collision pair",
			mappings: []*protos.TableMapping{
				{
					SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc1"},
					DestinationTable: &protos.QualifiedTable{Namespace: "a", Table: "b.c"},
				},
				{
					SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc2"},
					DestinationTable: &protos.QualifiedTable{Namespace: "a.b", Table: "c"},
				},
			},
			errContains: "ambiguous",
		},
		{
			name: "source dotted collision pair",
			mappings: []*protos.TableMapping{
				{
					SourceTable:      &protos.QualifiedTable{Namespace: "a", Table: "b.c"},
					DestinationTable: &protos.QualifiedTable{Table: "vdst1"},
				},
				{
					SourceTable:      &protos.QualifiedTable{Namespace: "a.b", Table: "c"},
					DestinationTable: &protos.QualifiedTable{Table: "vdst2"},
				},
			},
			errContains: "ambiguous",
		},
		{
			name: "empty destination table component",
			mappings: []*protos.TableMapping{
				{
					SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc1"},
					DestinationTable: &protos.QualifiedTable{Namespace: "a", Table: ""},
				},
			},
			errContains: "destination table name is empty",
		},
		{
			name: "empty source table component",
			mappings: []*protos.TableMapping{
				{
					SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: ""},
					DestinationTable: &protos.QualifiedTable{Table: "vdst"},
				},
			},
			errContains: "source table name is empty",
		},
	}

	for _, tc := range tests {
		s.t.Run(tc.name, func(t *testing.T) {
			flowConnConfig := &protos.FlowConnectionConfigs{
				FlowJobName:       "dotted_validation_" + s.suffix,
				SourceName:        sourcePeerName,
				DestinationName:   destinationPeerName,
				TableMappings:     tc.mappings,
				DoInitialSnapshot: true,
			}

			response, err := s.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
			require.Error(t, err, "expected validation to fail for %s", tc.name)
			require.Nil(t, response)

			st, ok := status.FromError(err)
			require.True(t, ok, "expected gRPC status error")
			require.Equal(t, codes.InvalidArgument, st.Code())
			require.Contains(t, st.Message(), tc.errContains)
		})
	}
}

// SkipValidation skips environmental checks only; structural identifier invariants
// must be enforced on create regardless
func (s APITestSuite) TestMirrorValidation_StructuralChecksDespiteSkipValidation() {
	skipValidation := true
	flowConnConfig := &protos.FlowConnectionConfigs{
		FlowJobName:     "dotted_skip_validation_" + s.suffix,
		SourceName:      s.source.GeneratePeer(s.t).Name,
		DestinationName: s.ch.Peer().Name,
		TableMappings: []*protos.TableMapping{
			{
				SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc1"},
				DestinationTable: &protos.QualifiedTable{Namespace: "a", Table: "b.c"},
			},
			{
				SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: "vsrc2"},
				DestinationTable: &protos.QualifiedTable{Namespace: "a.b", Table: "c"},
			},
		},
		SkipValidation: &skipValidation,
	}

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.Error(s.t, err)
	require.Nil(s.t, response)
	st, ok := status.FromError(err)
	require.True(s.t, ok, "expected gRPC status error")
	require.Equal(s.t, codes.InvalidArgument, st.Code())
	require.Contains(s.t, st.Message(), "ambiguous")
}

func (s PeerFlowE2ETestSuitePG) Test_Dotted_Watermark_QRep() {
	srcSchema := "e2e_dot.qrep_" + s.suffix
	srcTable := "wat.ermark"
	dstTable := "wat.ermark_dst"
	srcQuoted := common.QuoteIdentifier(srcSchema) + "." + common.QuoteIdentifier(srcTable)
	dstQuoted := common.QuoteIdentifier(srcSchema) + "." + common.QuoteIdentifier(dstTable)

	_, err := s.Conn().Exec(s.t.Context(), "CREATE SCHEMA "+common.QuoteIdentifier(srcSchema))
	require.NoError(s.t, err)
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = s.Conn().Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", common.QuoteIdentifier(srcSchema)))
	})

	for _, table := range []string{srcQuoted, dstQuoted} {
		_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"CREATE TABLE %s (id SERIAL PRIMARY KEY, val TEXT NOT NULL, updated_at TIMESTAMP NOT NULL DEFAULT now())", table))
		require.NoError(s.t, err)
	}
	for i := range 10 {
		_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"INSERT INTO %s (val) VALUES ('val_%d')", srcQuoted, i))
		require.NoError(s.t, err)
	}

	qrepConfig := &protos.QRepConfig{
		FlowJobName:             AddSuffix(s, "dotted_qrep"),
		QualifiedWatermarkTable: &protos.QualifiedTable{Namespace: srcSchema, Table: srcTable},
		DestinationTable:        &protos.QualifiedTable{Namespace: srcSchema, Table: dstTable},
		SourceName:              GeneratePostgresPeer(s.t).Name,
		DestinationName:         GeneratePostgresPeer(s.t).Name,
		Query:                   fmt.Sprintf("SELECT * FROM %s WHERE updated_at BETWEEN {{.start}} AND {{.end}}", srcQuoted),
		WatermarkColumn:         "updated_at",
		WriteMode: &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		},
		NumRowsPerPartition: 100,
		InitialCopyOnly:     true,
		Version:             shared.InternalVersion_Latest,
	}

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	require.NoError(s.t, s.comparePGTables(srcQuoted, dstQuoted, "id,val,updated_at"))
}

func (s MongoClickhouseSuite) Test_Dotted_Collection_Name() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "col.lection"
	dstTable := "col.lection_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, "dotted_collection"),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)
	for i := range 5 {
		res, err := collection.InsertOne(t.Context(),
			bson.D{bson.E{Key: fmt.Sprintf("init_key_%d", i), Value: fmt.Sprintf("init_value_%d", i)}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial load to match", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	for i := range 5 {
		res, err := collection.InsertOne(t.Context(),
			bson.D{bson.E{Key: fmt.Sprintf("cdc_key_%d", i), Value: fmt.Sprintf("cdc_value_%d", i)}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	EnvWaitForEqualTablesWithNames(env, s, "cdc events to match", srcTable, dstTable, "_id,doc")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// QRep over a dotted PARTITIONED parent with ctid partitioning: partitions carry
// ChildTableRange structs for the dotted children (plan 7.B.5).
func (s PeerFlowE2ETestSuitePG) Test_Dotted_Partitioned_Parent_QRep() {
	srcSchema := "e2e_dot.part_" + s.suffix
	parent := common.QualifiedTable{Namespace: srcSchema, Table: "pa.rent"}
	dst := common.QualifiedTable{Namespace: srcSchema, Table: "pa.rent_dst"}
	parentQuoted := parent.String()
	dstQuoted := dst.String()

	_, err := s.Conn().Exec(s.t.Context(), "CREATE SCHEMA "+common.QuoteIdentifier(srcSchema))
	require.NoError(s.t, err)
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = s.Conn().Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", common.QuoteIdentifier(srcSchema)))
	})

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL,
			partition_key INT NOT NULL,
			val TEXT NOT NULL,
			PRIMARY KEY (partition_key, id)
		) PARTITION BY RANGE (partition_key)`, parentQuoted))
	require.NoError(s.t, err)
	for i := range 3 {
		child := common.QualifiedTable{Namespace: srcSchema, Table: fmt.Sprintf("chi.ld_%d", i)}
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)",
			child.String(), parentQuoted, i*20, (i+1)*20))
		require.NoError(s.t, err)
	}
	for i := range 60 {
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
			"INSERT INTO %s (partition_key, val) VALUES (%d, 'val_%d')", parentQuoted, i, i))
		require.NoError(s.t, err)
	}

	flowName := AddSuffix(s, "dotted_part_qrep")
	qrepConfig := &protos.QRepConfig{
		FlowJobName:             flowName,
		ParentMirrorName:        flowName,
		QualifiedWatermarkTable: internal.QualifiedTableProto(parent),
		DestinationTable:        internal.QualifiedTableProto(dst),
		SourceName:              GeneratePostgresPeer(s.t).Name,
		DestinationName:         GeneratePostgresPeer(s.t).Name,
		Query:                   fmt.Sprintf("SELECT * FROM %s WHERE ctid BETWEEN {{.start}} AND {{.end}}", parentQuoted),
		WatermarkColumn:         "ctid",
		WriteMode: &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		},
		NumRowsPerPartition: 10,
		InitialCopyOnly:     true,
		// child-range pulls resolve the destination schema from the catalog, which is
		// populated by the watermark-table setup step (it also creates the dotted
		// destination table)
		SetupWatermarkTableOnDestination: true,
		Version:                          shared.InternalVersion_Latest,
	}

	tc := NewTemporalClient(s.t)
	env := RunQRepFlowWorkflow(s.t, tc, qrepConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	require.NoError(s.t, env.Error(s.t.Context()))

	require.NoError(s.t, s.comparePGTables(parentQuoted, dstQuoted, "id,partition_key,val"))
}

// Initial-snapshot-only mirror with dotted names: the snapshot/clone workflow path
// builds child workflow IDs from the dotted source and the flow must reach COMPLETED
// (plan 7.B.9).
func (s APITestSuite) TestInitialSnapshotOnlyWithDottedNames() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("dotted source identifiers are only legal on Postgres sources")
	}

	srcTable := "snap.only"
	dstTable := "snap.only_dst"
	srcQuoted := common.QuoteIdentifier(Schema(s)) + "." + common.QuoteIdentifier(srcTable)

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", srcQuoted)))
	for i := range 10 {
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) VALUES (%d,'snap_%d')", srcQuoted, i, i)))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "dotted_snapshot_only_" + s.suffix,
		TableMappings: []*protos.TableMapping{{
			SourceTable:      &protos.QualifiedTable{Namespace: Schema(s), Table: srcTable},
			DestinationTable: &protos.QualifiedTable{Table: dstTable},
			ShardingKey:      "id",
		}},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = true

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitForFinished(s.t, env, 3*time.Minute)
	EnvWaitForEqualTablesWithNames(env, s.ch, "dotted snapshot-only initial load", srcTable, dstTable, "id,val")
}
