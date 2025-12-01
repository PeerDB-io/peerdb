package e2e

import (
	"fmt"
	"sync"
	"time"

	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	tp "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/proto"
)

/*
removeOneTable pauses the flow, removes the specified table, and waits for the flow to be running again,
assumes source and target tables have same name
*/
func (s APITestSuite) removeOneTable(
	env WorkflowRun,
	flowJobName string,
	tableToRemove *protos.TableMapping,
	expectedRemainingSourceTables []string,
) {
	// Pause the flow
	_, err := s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, fmt.Sprintf(
		"wait for pause for remove %s", tableToRemove.SourceTableIdentifier), func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	// Remove the table
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					RemovedTables: []*protos.TableMapping{tableToRemove},
				},
			},
		},
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, fmt.Sprintf(
		"wait for table removal of %s to finish", tableToRemove), func() bool {
		valid, err := s.checkCatalogTableMapping(s.t.Context(),
			s.pg.PostgresConnector.Conn(), flowJobName, expectedRemainingSourceTables)
		if err != nil {
			return false
		}
		return valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
}

/*
addOneTable pauses the flow, adds the specified table, and waits for the flow to be running again,
assumes source and target tables have same name
*/
func (s APITestSuite) addOneTable(
	env WorkflowRun,
	flowJobName string,
	tableToAdd *protos.TableMapping,
) {
	// Pause the flow
	_, err := s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, fmt.Sprintf("wait for pause for add %s", tableToAdd.SourceTableIdentifier), func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	// Add the table
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{tableToAdd},
				},
			},
		},
	})
	require.NoError(s.t, err)
}

func (s APITestSuite) checkPublicationTables(
	publicationName string,
	includedTables []*utils.SchemaTable,
	cancelledTables []*utils.SchemaTable,
) {

	pubTables, err := s.pg.PostgresConnector.GetTablesFromPublication(
		s.t.Context(), publicationName, nil)
	require.NoError(s.t, err, "error getting publication tables for included tables")
	var pubTableSet = make(map[string]struct{})
	for _, table := range pubTables {
		pubTableSet[table.String()] = struct{}{}
	}
	for _, table := range cancelledTables {
		_, exists := pubTableSet[table.String()]
		require.False(s.t, exists,
			"expected publication to not contain cancelled table %s", table.String())
	}
	for _, table := range includedTables {
		_, exists := pubTableSet[table.String()]
		require.True(s.t, exists,
			"expected publication to contain included table %s", table.String())
	}
}

type includedTable struct {
	tableName string
	entries   int
}

func (s APITestSuite) checkQrepRuns(
	flowJobName string,
	expectedTables []includedTable,
) {
	for _, tableEntry := range expectedTables {
		var actualCount int
		queryErr := s.pg.PostgresConnector.Conn().QueryRow(s.t.Context(),
			`SELECT COUNT(*) FROM peerdb_stats.qrep_runs 
            WHERE parent_mirror_name=$1 AND source_table = $2 AND consolidate_complete = true`,
			flowJobName,
			tableEntry.tableName,
		).Scan(&actualCount)
		require.NoError(s.t, queryErr,
			fmt.Sprintf("error querying qrep_runs for table %s", tableEntry.tableName))
		require.Equal(s.t, tableEntry.entries, actualCount,
			fmt.Sprintf("expected %d qrep_runs entries for table %s, got %d",
				tableEntry.entries, tableEntry.tableName, actualCount))
	}
}

func (s APITestSuite) checkQrepPartitions(
	flowJobName string,
	expectedTables []includedTable,
) {
	for _, tableEntry := range expectedTables {
		var actualCount int
		queryErr := s.pg.PostgresConnector.Conn().QueryRow(s.t.Context(),
			`SELECT COUNT(*) FROM peerdb_stats.qrep_partitions qp
            JOIN peerdb_stats.qrep_runs qr ON qr.parent_mirror_name = qp.parent_mirror_name AND qr.run_uuid = qp.run_uuid
            WHERE qr.parent_mirror_name = $1 AND qr.source_table = $2 AND qr.consolidate_complete = true`,
			flowJobName,
			tableEntry.tableName,
		).Scan(&actualCount)
		require.NoError(s.t, queryErr,
			fmt.Sprintf("error querying qrep_partitions for table %s", tableEntry.tableName))

		if tableEntry.entries > 0 {
			require.Greater(s.t, actualCount, 0,
				fmt.Sprintf("expected qrep_partitions entries for table %s, got %d",
					tableEntry.tableName, actualCount))
		} else {
			require.Equal(s.t, 0, actualCount,
				fmt.Sprintf("expected no qrep_partitions entries for table %s, got %d",
					tableEntry.tableName, actualCount))
		}
	}
}

func (s APITestSuite) checkTableSchemaMapping(
	flowJobName string,
	expectedTables []includedTable,
) {
	for _, tableEntry := range expectedTables {
		destinationTableName := tableEntry.tableName
		var actualCount int
		queryErr := s.pg.PostgresConnector.Conn().QueryRow(s.t.Context(),
			`SELECT COUNT(*) FROM table_schema_mapping 
            WHERE flow_name = $1 AND table_name = $2`,
			flowJobName,
			destinationTableName,
		).Scan(&actualCount)
		require.NoError(s.t, queryErr,
			fmt.Sprintf("error querying table_schema_mapping for table %s", destinationTableName))

		if tableEntry.entries > 0 {
			require.Equal(s.t, 1, actualCount,
				fmt.Sprintf("expected 1 table_schema_mapping entry for table %s, got %d",
					destinationTableName, actualCount))
		} else {
			require.Equal(s.t, 0, actualCount,
				fmt.Sprintf("expected no table_schema_mapping entry for table %s, got %d",
					destinationTableName, actualCount))
		}
	}
}

func (s APITestSuite) testCancelTableAddition(
	assumeTableRemovalWillNotHappen bool, withRemoval bool) {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t2"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t3"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t4"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t5"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t6"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t2"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t3"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t4"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t5"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t6"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t3").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t4").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t5").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t6").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	flowName := "cancel_table_addition_test_flow"
	// based on the two inputs
	if assumeTableRemovalWillNotHappen && !withRemoval {
		flowName += "_no_removal_assumed"
	} else if !assumeTableRemovalWillNotHappen && withRemoval {
		flowName += "_with_removal"
	} else if assumeTableRemovalWillNotHappen && withRemoval {
		flowName += "_no_removal_assumed_with_removal"
	} else {
		flowName += "_normal"
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: flowName + "_" + s.suffix,
		TableNameMapping: map[string]string{
			AttachSchema(s, "t1"): "t1",
			AttachSchema(s, "t2"): "t2",
			AttachSchema(s, "t3"): "t3",
		},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	// verify initial tables are equal
	EnvWaitForEqualTables(env, s.ch, "t1 initial load", "t1", cols)
	EnvWaitForEqualTables(env, s.ch, "t2 initial load", "t2", cols)
	EnvWaitForEqualTables(env, s.ch, "t3 initial load", "t3", cols)

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	t5Mv := s.ch.NewMVManager("t5", s.suffix)
	err = s.ch.CreateRMTTable("t5", []TestClickHouseColumn{
		{
			Name: "id",
			Type: "Int64",
		},
		{
			Name: "val",
			Type: "String",
		},
	}, "id")
	require.NoError(s.t, err)
	err = t5Mv.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)

	tableModification := &protos.CDCFlowConfigUpdate{
		AdditionalTables: []*protos.TableMapping{
			{
				SourceTableIdentifier:      AttachSchema(s, "t4"),
				DestinationTableIdentifier: "t4",
			},
			{
				SourceTableIdentifier:      AttachSchema(s, "t5"),
				DestinationTableIdentifier: "t5",
			},
			{
				SourceTableIdentifier:      AttachSchema(s, "t6"),
				DestinationTableIdentifier: "t6",
			},
		},
	}
	if withRemoval {
		tableModification.RemovedTables = []*protos.TableMapping{
			{
				SourceTableIdentifier:      AttachSchema(s, "t2"),
				DestinationTableIdentifier: "t2",
			},
			{
				SourceTableIdentifier:      AttachSchema(s, "t3"),
				DestinationTableIdentifier: "t3",
			},
		}
	}

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: tableModification,
			},
		},
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for snapshot of add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})
	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for initial load MV error messages for t5", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, t5Mv.mvName),
		)
		return err == nil && count > 0
	})

	currentlyReplicatingTables := []*protos.TableMapping{
		{
			SourceTableIdentifier:      AttachSchema(s, "t1"),
			DestinationTableIdentifier: "t1",
		},
		{
			SourceTableIdentifier:      AttachSchema(s, "t4"),
			DestinationTableIdentifier: "t4",
		},
		{
			SourceTableIdentifier:      AttachSchema(s, "t5"),
			DestinationTableIdentifier: "t5",
		},
		{
			SourceTableIdentifier:      AttachSchema(s, "t6"),
			DestinationTableIdentifier: "t6",
		},
	}
	if !withRemoval {
		currentlyReplicatingTables = append(currentlyReplicatingTables, &protos.TableMapping{
			SourceTableIdentifier:      AttachSchema(s, "t2"),
			DestinationTableIdentifier: "t2",
		})
		currentlyReplicatingTables = append(currentlyReplicatingTables, &protos.TableMapping{
			SourceTableIdentifier:      AttachSchema(s, "t3"),
			DestinationTableIdentifier: "t3",
		})
	}
	output, err := s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
		FlowJobName:                     flowConnConfig.FlowJobName,
		CurrentlyReplicatingTables:      currentlyReplicatingTables,
		IdempotencyKey:                  s.suffix,
		AssumeTableRemovalWillNotHappen: assumeTableRemovalWillNotHappen,
	})

	if !assumeTableRemovalWillNotHappen && withRemoval {
		require.ErrorContains(s.t, err, "please set assume_table_removal_will_not_happen")
		env.Cancel(s.t.Context())
		RequireEnvCanceled(s.t, env)
		return
	} else {
		require.NoError(s.t, err)
	}

	var outputSourceTables []string
	for _, table := range output.TablesAfterCancellation {
		outputSourceTables = append(outputSourceTables, table.SourceTableIdentifier)
	}
	expectedTables := []string{
		AttachSchema(s, "t1"),
		AttachSchema(s, "t2"),
		AttachSchema(s, "t3"),
		AttachSchema(s, "t4"),
	}

	require.ElementsMatch(s.t, expectedTables, outputSourceTables,
		"expected tables after cancellation to match")

	// check catalog and publication
	s.checkQrepRuns(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 1},
			{tableName: AttachSchema(s, "t3"), entries: 1},
			{tableName: AttachSchema(s, "t4"), entries: 1},
		},
	)
	s.checkQrepRuns(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 1},
			{tableName: AttachSchema(s, "t3"), entries: 1},
			{tableName: AttachSchema(s, "t4"), entries: 1},
		},
	)
	s.checkTableSchemaMapping(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: "t1", entries: 1},
			{tableName: "t2", entries: 1},
			{tableName: "t3", entries: 1},
			{tableName: "t4", entries: 1},
		},
	)
	publicationName := connpostgres.GetDefaultPublicationName(flowConnConfig.FlowJobName)
	if _, ok := s.source.(*PostgresSource); ok {
		s.checkPublicationTables(
			publicationName,
			[]*utils.SchemaTable{
				{Schema: Schema(s), Table: "t1"},
				{Schema: Schema(s), Table: "t2"},
				{Schema: Schema(s), Table: "t3"},
				{Schema: Schema(s), Table: "t4"},
			},
			[]*utils.SchemaTable{
				{Schema: Schema(s), Table: "t5"},
				{Schema: Schema(s), Table: "t6"},
			},
		)
	}

	// drop target table
	err = s.ch.DropTable("t5")
	require.NoError(s.t, err)
	err = s.ch.DropTable("t6")
	require.NoError(s.t, err)

	// insert a row into all original tables
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t2"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t3"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t4"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t5"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t6"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t3").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t4").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t5").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t6").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t1", "t1", cols)
	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t2", "t2", cols)
	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t3", "t3", cols)
	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t4", "t4", cols)

	// Batch finished without erroring on t5 and t6 that don't have a target table anymore
	s.checkMetadataLastSyncStateValues(env, flowConnConfig, "batch id check after cdc", 1, 1)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestCancelTableAddition_NoRemovalAssumed() {
	s.testCancelTableAddition(false, false)
}

func (s APITestSuite) TestCancelTableAddition_WithRemoval() {
	s.testCancelTableAddition(false, true)
}

func (s APITestSuite) TestCancelTableAddition_NoRemovalAssumedWithRemoval() {
	s.testCancelTableAddition(true, true)
}

func (s APITestSuite) TestCancelTableAdditionRemoveAddRemove() {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t2"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t2"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "test_cancel_remove_add_remove" + "_" + s.suffix,
		TableNameMapping: map[string]string{
			AttachSchema(s, "t1"): "t1",
			AttachSchema(s, "t2"): "t2",
		},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	// verify initial tables are equal
	EnvWaitForEqualTables(env, s.ch, "t1", "t1", cols)
	EnvWaitForEqualTables(env, s.ch, "t2", "t2", cols)

	// remove t2
	s.removeOneTable(env, flowConnConfig.FlowJobName, &protos.TableMapping{
		SourceTableIdentifier:      AttachSchema(s, "t2"),
		DestinationTableIdentifier: "t2",
	}, []string{AttachSchema(s, "t1")})
	// re-add t2
	s.addOneTable(env, flowConnConfig.FlowJobName, &protos.TableMapping{
		SourceTableIdentifier:      AttachSchema(s, "t2"),
		DestinationTableIdentifier: "t2",
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for re-add t2", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	// remove t2 again
	s.removeOneTable(env, flowConnConfig.FlowJobName, &protos.TableMapping{
		SourceTableIdentifier:      AttachSchema(s, "t2"),
		DestinationTableIdentifier: "t2",
	}, []string{AttachSchema(s, "t1")})
	t2Mv := s.ch.NewMVManager("t2", s.suffix)
	require.NoError(s.t, err)
	err = t2Mv.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)
	// second time re-add t2
	s.addOneTable(env, flowConnConfig.FlowJobName, &protos.TableMapping{
		SourceTableIdentifier:      AttachSchema(s, "t2"),
		DestinationTableIdentifier: "t2",
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for stuck snapshot of t2 add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})
	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for initial load MV error messages for t2", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, t2Mv.mvName),
		)
		return err == nil && count > 0
	})
	output, err := s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
		FlowJobName: flowConnConfig.FlowJobName,
		CurrentlyReplicatingTables: []*protos.TableMapping{
			{SourceTableIdentifier: AttachSchema(s, "t1"), DestinationTableIdentifier: "t1"},
			{SourceTableIdentifier: AttachSchema(s, "t2"), DestinationTableIdentifier: "t2"},
		},
		IdempotencyKey:                  s.suffix,
		AssumeTableRemovalWillNotHappen: false,
	})
	require.NoError(s.t, err)

	var outputSourceTables []string
	for _, table := range output.TablesAfterCancellation {
		outputSourceTables = append(outputSourceTables, table.SourceTableIdentifier)
	}
	expectedTables := []string{
		AttachSchema(s, "t1"),
	}

	require.ElementsMatch(s.t, expectedTables, outputSourceTables,
		"expected tables after cancellation to match")

	s.checkQrepRuns(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 2},
		},
	)
	s.checkQrepPartitions(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 2}, // initial snapshot and first add but not the second add
		},
	)
	s.checkTableSchemaMapping(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: "t1", entries: 1},
			{tableName: "t2", entries: 0},
		},
	)
	publicationName := connpostgres.GetDefaultPublicationName(flowConnConfig.FlowJobName)
	if _, ok := s.source.(*PostgresSource); ok {
		s.checkPublicationTables(
			publicationName,
			[]*utils.SchemaTable{
				{Schema: Schema(s), Table: "t1"},
			},
			[]*utils.SchemaTable{
				{Schema: Schema(s), Table: "t2"},
			},
		)
	}

	require.NoError(s.t, err)
	err = s.ch.DropTable("t2")
	require.NoError(s.t, err)

	// insert a row into all original tables
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t2"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t1", "t1", cols)

	s.checkMetadataLastSyncStateValues(env, flowConnConfig, "batch id check after cdc", 1, 1)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
func (s APITestSuite) TestCancelAddCancel() {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t2"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t2"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "test_cancel_add_cancel" + "_" + s.suffix,
		TableNameMapping: map[string]string{
			AttachSchema(s, "t1"): "t1",
		},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	// verify initial tables are equal
	EnvWaitForEqualTables(env, s.ch, "t1", "t1", cols)

	t2Mv1 := s.ch.NewMVManager("t2", "first")
	err = s.ch.CreateRMTTable("t2", []TestClickHouseColumn{
		{
			Name: "id",
			Type: "Int64",
		},
		{
			Name: "val",
			Type: "String",
		},
	}, "id")
	require.NoError(s.t, err)
	err = t2Mv1.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)

	s.addOneTable(env, flowConnConfig.FlowJobName, &protos.TableMapping{
		SourceTableIdentifier:      AttachSchema(s, "t2"),
		DestinationTableIdentifier: "t2",
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for stuck snapshot of t2 add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})
	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for initial load MV error messages for t2", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, t2Mv1.mvName),
		)
		return err == nil && count > 0
	})

	output, err := s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
		FlowJobName: flowConnConfig.FlowJobName,
		CurrentlyReplicatingTables: []*protos.TableMapping{
			{SourceTableIdentifier: AttachSchema(s, "t1"), DestinationTableIdentifier: "t1"},
			{SourceTableIdentifier: AttachSchema(s, "t2"), DestinationTableIdentifier: "t2"},
		},
		IdempotencyKey:                  "same",
		AssumeTableRemovalWillNotHappen: false,
	})
	require.NoError(s.t, err)

	firstRunId := output.RunId
	var outputSourceTables []string
	for _, table := range output.TablesAfterCancellation {
		outputSourceTables = append(outputSourceTables, table.SourceTableIdentifier)
	}
	expectedTables := []string{
		AttachSchema(s, "t1"),
	}

	require.ElementsMatch(s.t, expectedTables, outputSourceTables,
		"expected tables after cancellation to match")

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for running after t2 cancel", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	err = t2Mv1.DropBadMV(s.t.Context())
	require.NoError(s.t, err)
	t2Mv2 := s.ch.NewMVManager("t2", "second")
	err = t2Mv2.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)

	s.addOneTable(env, flowConnConfig.FlowJobName, &protos.TableMapping{
		SourceTableIdentifier:      AttachSchema(s, "t2"),
		DestinationTableIdentifier: "t2",
	})
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for stuck snapshot of t2 add table again", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})
	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for initial load MV error messages for t2 second time", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, t2Mv2.mvName),
		)
		return err == nil && count > 0
	})

	// cancel again
	output, err = s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
		FlowJobName: flowConnConfig.FlowJobName,
		CurrentlyReplicatingTables: []*protos.TableMapping{
			{SourceTableIdentifier: AttachSchema(s, "t1"), DestinationTableIdentifier: "t1"},
			{SourceTableIdentifier: AttachSchema(s, "t2"), DestinationTableIdentifier: "t2"},
		},
		IdempotencyKey:                  "same",
		AssumeTableRemovalWillNotHappen: false,
	})
	require.NoError(s.t, err)

	expectedTables = []string{
		AttachSchema(s, "t1"),
	}

	outputSourceTables = []string{}
	for _, table := range output.TablesAfterCancellation {
		outputSourceTables = append(outputSourceTables, table.SourceTableIdentifier)
	}

	require.ElementsMatch(s.t, expectedTables, outputSourceTables,
		"expected tables after second cancellation to match")

	secondRunId := output.RunId
	require.Equal(s.t, firstRunId, secondRunId, "expected same run id for idempotent cancel")
	err = t2Mv2.DropBadMV(s.t.Context())
	require.NoError(s.t, err)
	t2Mv3 := s.ch.NewMVManager("t2", "third")
	err = t2Mv3.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for stuck snapshot of t2 add table third time", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})
	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for initial load MV error messages for t2 third time", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, t2Mv3.mvName),
		)
		return err == nil && count > 0
	})
	// cancel again
	output, err = s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
		FlowJobName: flowConnConfig.FlowJobName,
		CurrentlyReplicatingTables: []*protos.TableMapping{
			{SourceTableIdentifier: AttachSchema(s, "t1"), DestinationTableIdentifier: "t1"},
			{SourceTableIdentifier: AttachSchema(s, "t2"), DestinationTableIdentifier: "t2"},
		},
		IdempotencyKey:                  "different",
		AssumeTableRemovalWillNotHappen: false,
	})
	require.NoError(s.t, err)
	thirdRunId := output.RunId
	require.NotEqual(s.t, secondRunId, thirdRunId)
	outputSourceTables = []string{}
	for _, table := range output.TablesAfterCancellation {
		outputSourceTables = append(outputSourceTables, table.SourceTableIdentifier)
	}
	expectedTables = []string{
		AttachSchema(s, "t1"),
	}

	require.ElementsMatch(s.t, expectedTables, outputSourceTables,
		"expected tables after third cancellation to match")
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for running after third t2 cancel", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	s.checkQrepRuns(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 0},
		},
	)
	s.checkQrepPartitions(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 0},
		},
	)
	s.checkTableSchemaMapping(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: "t1", entries: 1},
			{tableName: "t2", entries: 0},
		},
	)

	require.NoError(s.t, err)
	err = s.ch.DropTable("t2")
	require.NoError(s.t, err)

	// insert a row into all original tables
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t1"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t2"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t1").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("t2").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t1", "t1", cols)

	s.checkMetadataLastSyncStateValues(env, flowConnConfig, "batch id check after cdc", 1, 1)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)

}
func (s APITestSuite) TestCancelTableAdditionDuringSetupFlow() {
	var cols string
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "original"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "added"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "original"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "added"))))
		cols = "id,val"
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("original").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		res, err = s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("added").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "cancel_table_addition_test_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, "original"): "original"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)
	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "original", cols)
	// add table
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for add table", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	// insert CDC row
	switch s.source.(type) {
	case *PostgresSource, *MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "original"))))
	case *MongoSource:
		res, err := s.Source().(*MongoSource).AdminClient().
			Database(Schema(s)).Collection("original").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 2}, bson.E{Key: "val", Value: "second"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}

	originalConfig := s.ch.Peer().GetClickhouseConfig()
	badClickHouseConfig := proto.Clone(originalConfig).(*protos.ClickhouseConfig)
	badClickHouseConfig.Host = "nonexistent-host"
	// Edit  ClickHouse peer to bad ClickHouse peer
	_, err = s.CreatePeer(s.t.Context(), &protos.CreatePeerRequest{
		Peer: &protos.Peer{
			Name: flowConnConfig.DestinationName,
			Type: protos.DBType_CLICKHOUSE,
			Config: &protos.Peer_ClickhouseConfig{
				ClickhouseConfig: badClickHouseConfig,
			},
		},
		DisableValidation: true,
		AllowUpdate:       true,
	})
	require.NoError(s.t, err)
	s.t.Log("Edited peer with bad host")

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{
						{
							SourceTableIdentifier:      AttachSchema(s, "added"),
							DestinationTableIdentifier: "added",
						},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table addition to be stuck", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})

	output, err := s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
		FlowJobName:                     flowConnConfig.FlowJobName,
		CurrentlyReplicatingTables:      flowConnConfig.TableMappings,
		IdempotencyKey:                  s.suffix,
		AssumeTableRemovalWillNotHappen: false,
	})
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table addition cancellation to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	expectedTables := []string{AttachSchema(s, "original")}
	var outputSourceTables []string
	for _, table := range output.TablesAfterCancellation {
		outputSourceTables = append(outputSourceTables, table.SourceTableIdentifier)
	}
	require.ElementsMatch(s.t, expectedTables, outputSourceTables,
		"expected tables after cancellation to match")

	s.checkTableSchemaMapping(flowConnConfig.FlowJobName, []includedTable{
		{
			tableName: "original",
			entries:   1,
		},
		{
			tableName: "added",
			entries:   0,
		},
	})

	// pause
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause for peer restore", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	// Restore original ClickHouse peer config
	_, err = s.CreatePeer(s.t.Context(), &protos.CreatePeerRequest{
		Peer: &protos.Peer{
			Name: s.ch.Peer().Name,
			Type: protos.DBType_CLICKHOUSE,
			Config: &protos.Peer_ClickhouseConfig{
				ClickhouseConfig: originalConfig,
			},
		},
		AllowUpdate: true,
	})
	require.NoError(s.t, err)

	// resume
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for resume post peer restore", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// wait for equal tables on original
	EnvWaitForEqualTables(env, s.ch, "original after peer restore", "original", cols)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s APITestSuite) TestDoubleClickCancelTableAddition() {
	// Test: use Toxiproxy to ensure double cancel works correctly

	// To run locally, requires toxiproxy running:
	// docker run -d \
	//   --name peerdb-toxiproxy \
	//   -p 18474:8474 \
	//   -p 9904:9902 \
	//   ghcr.io/shopify/toxiproxy:2.11.0

	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL")
	}

	suffix := "double_cancel_" + s.suffix
	pgWithProxy, proxy, err := SetupPostgresWithToxiproxy(s.t, suffix, 9904)
	require.NoError(s.t, err)
	defer func() {
		require.NoError(s.t, proxy.Enable())
		pgWithProxy.Teardown(s.t, s.t.Context(), suffix)
	}()

	var cols string
	// Create tables in PostgreSQL
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t1"))))
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, "t2"))))
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t1"))))
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, "t2"))))
	cols = "id,val"

	// Create peer for the proxy connection
	proxyConfig := internal.GetCatalogPostgresConfigFromEnv(s.t.Context())
	proxyConfig.Port = uint32(9904)
	proxyPeer := &protos.Peer{
		Name: "proxy_postgres_" + suffix,
		Type: protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{
			PostgresConfig: proxyConfig,
		},
	}
	CreatePeer(s.t, proxyPeer)
	defer func() {
		_, _ = s.DropPeer(s.t.Context(), &protos.DropPeerRequest{PeerName: proxyPeer.Name})
	}()

	// Create snapshot+CDC mirror with t1
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "double_cancel_test_" + suffix,
		TableNameMapping: map[string]string{
			AttachSchema(s, "t1"): "t1",
		},
		Destination: s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SourceName = proxyPeer.Name

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial load to finish (status == Running)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "t1", cols)

	// Pause the mirror
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for pause", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	// Create table t2 in ClickHouse and bad MV
	err = s.ch.CreateRMTTable("t2", []TestClickHouseColumn{
		{
			Name: "id",
			Type: "Int64",
		},
		{
			Name: "val",
			Type: "String",
		},
	}, "id")
	require.NoError(s.t, err)

	t2Mv := s.ch.NewMVManager("t2", suffix)
	err = t2Mv.CreateBadMV(s.t.Context())
	require.NoError(s.t, err)

	// Flow state change request: Additional tables: t2
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{
						{
							SourceTableIdentifier:      AttachSchema(s, "t2"),
							DestinationTableIdentifier: "t2",
						},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)

	// Wait for mirror to be stuck on clone of t2
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for snapshot status", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_SNAPSHOT
	})
	EnvWaitFor(s.t, env, 5*time.Minute, "waiting for MV error messages for t2", func() bool {
		count, err := s.pg.GetLogCount(
			s.t.Context(), flowConnConfig.FlowJobName, "error",
			fmt.Sprintf("while pushing to view %s.%s", s.ch.connector.Config.Database, t2Mv.mvName),
		)
		return err == nil && count > 0
	})

	// Add latency toxic of 5 seconds
	const toxicDelay = 5 * time.Second
	toxic, err := proxy.AddToxic("latency", "latency", "downstream", 1.0, tp.Attributes{
		"latency": int(toxicDelay.Milliseconds()),
	})
	require.NoError(s.t, err)

	// Call 2 times in goroutines: table addition cancellation
	var wg sync.WaitGroup
	var output1, output2 *protos.CancelTableAdditionOutput
	var err1, err2 error
	var duration1, duration2 time.Duration

	wg.Add(2)
	go func() {
		defer wg.Done()
		start := time.Now()
		output1, err1 = s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
			FlowJobName: flowConnConfig.FlowJobName,
			CurrentlyReplicatingTables: []*protos.TableMapping{
				{SourceTableIdentifier: AttachSchema(s, "t1"), DestinationTableIdentifier: "t1"},
				{SourceTableIdentifier: AttachSchema(s, "t2"), DestinationTableIdentifier: "t2"},
			},
			IdempotencyKey:                  "1",
			AssumeTableRemovalWillNotHappen: false,
		})
		duration1 = time.Since(start)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		output2, err2 = s.CancelTableAddition(s.t.Context(), &protos.CancelTableAdditionInput{
			FlowJobName: flowConnConfig.FlowJobName,
			CurrentlyReplicatingTables: []*protos.TableMapping{
				{SourceTableIdentifier: AttachSchema(s, "t1"), DestinationTableIdentifier: "t1"},
				{SourceTableIdentifier: AttachSchema(s, "t2"), DestinationTableIdentifier: "t2"},
			},
			IdempotencyKey:                  "1",
			AssumeTableRemovalWillNotHappen: false,
		})
		duration2 = time.Since(start)
	}()

	// Let goroutines start with toxic active
	time.Sleep(toxicDelay)

	// Remove toxic so requests can complete
	err = proxy.RemoveToxic(toxic.Name)
	require.NoError(s.t, err)

	wg.Wait()

	// Measure both durations > 5 seconds
	require.Greater(s.t, duration1, toxicDelay)
	require.Greater(s.t, duration2, toxicDelay)

	// Verify both requests succeeded
	require.NoError(s.t, err1)
	require.NoError(s.t, err2)
	require.NotNil(s.t, output1)
	require.NotNil(s.t, output2)

	// Check that table mappings and run id is same for both
	require.Equal(s.t, output1.RunId, output2.RunId, "Both requests should return same run ID")
	require.ElementsMatch(s.t, output1.TablesAfterCancellation, output2.TablesAfterCancellation,
		"Both requests should return same table mappings")

	// Expected tables after cancellation: only t1
	expectedTables := []string{AttachSchema(s, "t1")}
	var output1SourceTables []string
	for _, table := range output1.TablesAfterCancellation {
		output1SourceTables = append(output1SourceTables, table.SourceTableIdentifier)
	}
	require.ElementsMatch(s.t, expectedTables, output1SourceTables)

	// Wait for flow to return to running
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for running after cancellation", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// Check qrep_runs (only 1 entry for t1), qrep_partitions (only 1 entry for t1), table_schema_mapping (yes t1, no t2)
	s.checkQrepRuns(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 0},
		},
	)
	s.checkQrepPartitions(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: AttachSchema(s, "t1"), entries: 1},
			{tableName: AttachSchema(s, "t2"), entries: 0},
		},
	)
	s.checkTableSchemaMapping(
		flowConnConfig.FlowJobName,
		[]includedTable{
			{tableName: "t1", entries: 1},
			{tableName: "t2", entries: 0},
		},
	)

	// Make sure publication does not have t2 and has t1
	publicationName := connpostgres.GetDefaultPublicationName(flowConnConfig.FlowJobName)
	s.checkPublicationTables(
		publicationName,
		[]*utils.SchemaTable{
			{Schema: Schema(s), Table: "t1"},
		},
		[]*utils.SchemaTable{
			{Schema: Schema(s), Table: "t2"},
		},
	)

	// Drop t2 on ClickHouse
	err = s.ch.DropTable("t2")
	require.NoError(s.t, err)
	err = t2Mv.DropBadMV(s.t.Context())
	require.NoError(s.t, err)

	// Insert a row into t1, t2 in PG
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t1"))))
	require.NoError(s.t, pgWithProxy.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (2,'second')", AttachSchema(s, "t2"))))

	// EnvWaitForEqualTables on t1 (t2 should not be replicated)
	EnvWaitForEqualTables(env, s.ch, "cdc after cancellation t1", "t1", cols)

	// Check if normalize batch id incremented in metadata_last_sync_state
	s.checkMetadataLastSyncStateValues(env, flowConnConfig, "batch id check after cdc", 1, 1)

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
