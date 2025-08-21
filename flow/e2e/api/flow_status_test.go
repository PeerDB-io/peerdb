package e2e_api

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type flowStatusUpdate struct {
	FlowJobName string `db:"flow_job_name"`
	OldStatus   string `db:"old_status"`
	NewStatus   string `db:"new_status"`
}

func (s Suite) getFlowStatusUpdates(flowJobName string) ([]flowStatusUpdate, error) {
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

func (s Suite) setupFlowStatusTestDependencies() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		`CREATE TABLE flow_status_updates (
			id serial PRIMARY KEY,
			flow_job_name text NOT NULL,
			old_status text NOT NULL,
			new_status text NOT NULL
		)`))
	// Track updates to status via a trigger
	require.NoError(s.t, s.source.Exec(s.t.Context(),
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
	))
}

func (s Suite) cleanupFlowStatusTestDependencies() {
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		"DROP TRIGGER IF EXISTS flow_status_update ON flows;"))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		"DROP FUNCTION IF EXISTS flow_status_update_trigger();"))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		"DROP TABLE IF EXISTS flow_status_updates;"))
}

func (s Suite) TestFlowStatusUpdate() {
	defer s.cleanupFlowStatusTestDependencies()
	s.setupFlowStatusTestDependencies()
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", e2e.AttachSchema(s, "status_test"))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", e2e.AttachSchema(s, "status_test"))))
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
	e2e.EnvWaitForFinished(s.t, env, 3*time.Minute)
	e2e.RequireEqualTables(s.ch, "status_test", "id,val")

	// pause the mirror
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(s.t, err)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "wait for paused state", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
	})

	s.checkMetadataLastSyncStateValues(env, flowConnConfig, "pause", 0, 0)

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
	require.Len(s.t, updates, 3, "expected exactly 3 status updates")
	require.Equal(s.t, protos.FlowStatus_STATUS_RUNNING.String(), updates[0].OldStatus,
		"expected first old status to be running")
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSING.String(), updates[0].NewStatus,
		"expected first new status to be pausing")
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSING.String(), updates[1].OldStatus,
		"expected second old status to be pausing")
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSED.String(), updates[1].NewStatus,
		"expected second new status to be paused")
	require.Equal(s.t, protos.FlowStatus_STATUS_PAUSED.String(), updates[2].OldStatus,
		"expected third old status to be paused")
	require.Equal(s.t, protos.FlowStatus_STATUS_RUNNING.String(), updates[2].NewStatus,
		"expected third new status to be running")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}
