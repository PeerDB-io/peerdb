package e2e_api

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	e2e_mongo "github.com/PeerDB-io/peerdb/flow/e2e/mongo"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type flowStatusUpdate struct {
	FlowJobName string            `db:"flow_job_name"`
	OldStatus   protos.FlowStatus `db:"old_status"`
	NewStatus   protos.FlowStatus `db:"new_status"`
}

func (s APITestSuite) getFlowStatusUpdates(flowJobName string) ([]flowStatusUpdate, error) {
	var updates []flowStatusUpdate
	rows, err := s.pg.PostgresConnector.Conn().Query(
		s.t.Context(),
		`SELECT flow_job_name, old_status, new_status
		FROM flow_status_updates
		WHERE flow_job_name = $1 AND old_status != new_status`,
		flowJobName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get flow status updates: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var update flowStatusUpdate
		if err := rows.Scan(&update.FlowJobName, &update.OldStatus, &update.NewStatus); err != nil {
			return nil, fmt.Errorf("failed to scan flow status update: %w", err)
		}
		updates = append(updates, update)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return updates, nil
}

func (s APITestSuite) setupFlowStatusTestDependencies() {
	_, err := s.pg.PostgresConnector.Conn().Exec(s.t.Context(),
		`CREATE TABLE IF NOT EXISTS flow_status_updates (
			id serial PRIMARY KEY,
			flow_job_name text NOT NULL,
			old_status integer NOT NULL,
			new_status integer NOT NULL
		)`)
	require.NoError(s.t, err)
	// Track updates to status via a trigger
	_, err = s.pg.PostgresConnector.Conn().Exec(s.t.Context(),
		// create trigger
		`CREATE OR REPLACE FUNCTION flow_status_update_trigger() RETURNS TRIGGER AS $$
			BEGIN
				INSERT INTO flow_status_updates (flow_job_name, old_status, new_status)
				VALUES (NEW.name, OLD.status, NEW.status);
				RETURN NEW;
			END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER flow_status_update
		AFTER UPDATE OF status ON flows
		FOR EACH ROW
		EXECUTE FUNCTION flow_status_update_trigger();`,
	)
	require.NoError(s.t, err)
}

func (s APITestSuite) TestFlowStatusUpdate() {
	s.setupFlowStatusTestDependencies()
	var cols string
	switch s.source.(type) {
	case *e2e.PostgresSource, *e2e.MySqlSource:
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "status_test"))))
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", e2e.AttachSchema(s, "status_test"))))
		cols = "id,val"
	case *e2e_mongo.MongoSource:
		res, err := s.Source().(*e2e_mongo.MongoSource).AdminClient().
			Database(e2e.Schema(s)).Collection("valid").
			InsertOne(s.t.Context(), bson.D{bson.E{Key: "id", Value: 1}, bson.E{Key: "val", Value: "first"}}, options.InsertOne())
		require.NoError(s.t, err)
		require.True(s.t, res.Acknowledged)
		cols = fmt.Sprintf("%s,%s", connmongo.DefaultDocumentKeyColumnName, connmongo.DefaultFullDocumentColumnName)
	default:
		require.Fail(s.t, fmt.Sprintf("unknown source type %T", s.source))
	}
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      "flow_status_update_" + s.suffix,
		TableNameMapping: map[string]string{e2e.AttachSchema(s, "status_test"): "status_test"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := e2e.NewTemporalClient(s.t)
	env, err := e2e.GetPeerflow(s.t.Context(), s.pg.PostgresConnector.Conn(), tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.RequireEqualTables(s.ch, "status_test", cols)

	// pause the mirror
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for paused state", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	// resume the mirror
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
	})
	require.NoError(s.t, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for running state", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	// check if flow status updates are recorded and are pausing, paused and running
	updates, err := s.getFlowStatusUpdates(flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	// SETUP -> SNAPSHOT
	// SNAPSHOT -> RUNNING
	// RUNNING -> PAUSING
	// PAUSING -> PAUSED
	// PAUSED -> RUNNING
	require.Len(s.t, updates, 5, "expected exactly 5 status updates")
	require.Equal(s.t, protos.FlowStatus_STATUS_SETUP, updates[0].OldStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_SNAPSHOT, updates[0].NewStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_SNAPSHOT, updates[1].OldStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_RUNNING, updates[1].NewStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_RUNNING, updates[2].OldStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSING, updates[2].NewStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSING, updates[3].OldStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSED, updates[3].NewStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSED, updates[4].OldStatus)
	require.Equal(s.t, protos.FlowStatus_STATUS_RUNNING, updates[4].NewStatus)
}
