package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// TestCustomPublicationNonOwnerUser verifies a mirror on a customer-managed publication,
// reading through a postgres user that does not own the publication, should support adding
// and removing tables, resync, and drop
func (s APITestSuite) TestCustomPublicationNonOwnerUser() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only for PostgreSQL")
	}
	ctx := s.t.Context()
	cols := "id,val"

	original := AttachSchema(s, "nonowner_original")
	added := AttachSchema(s, "nonowner_added")
	for _, table := range []string{original, added} {
		require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", table)))
		require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", table)))
	}

	// The suite's admin connection owns the publication; the mirror never logs in as it.
	pubName := "pub_nonowner_" + s.suffix
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, original)))
	s.t.Cleanup(func() {
		_ = s.source.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pubName)
	})

	roUser := "nonowner_" + s.suffix
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'nonowner' REPLICATION", roUser)))
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", Schema(s), roUser)))
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s", Schema(s), roUser)))
	s.t.Cleanup(func() {
		_ = s.source.Exec(context.Background(), "DROP OWNED BY "+roUser)
		_ = s.source.Exec(context.Background(), "DROP ROLE IF EXISTS "+roUser)
	})

	peerConfig := internal.GetAncillaryPostgresConfigFromEnv()
	peerConfig.User = roUser
	peerConfig.Password = "nonowner"
	peer := &protos.Peer{
		Name:   "nonowner_pg_" + s.suffix,
		Type:   protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{PostgresConfig: peerConfig},
	}
	peerResp, err := s.CreatePeer(ctx, &protos.CreatePeerRequest{Peer: peer})
	require.NoError(s.t, err)
	require.Equal(s.t, protos.CreatePeerStatus_CREATED, peerResp.Status, peerResp.Message)

	requirePublicationOwnedByAdmin := func() {
		s.t.Helper()
		var owner string
		require.NoError(s.t, s.pg.Conn().QueryRow(ctx,
			"SELECT pg_get_userbyid(pubowner) FROM pg_publication WHERE pubname = $1", pubName).Scan(&owner))
		require.NotEqual(s.t, roUser, owner, "the mirror login must not own the publication")
	}
	requirePublicationOwnedByAdmin()

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "nonowner_pub_" + s.suffix,
		TableNameMapping: map[string]string{original: "nonowner_original"},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SourceName = peer.Name
	flowConnConfig.PublicationName = pubName
	flowConnConfig.DoInitialSnapshot = true

	response, err := s.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(ctx, s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial load to finish", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "nonowner_original", cols)

	pause := func(env WorkflowRun, reason string) {
		s.t.Helper()
		_, err := s.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
			FlowJobName:        flowConnConfig.FlowJobName,
			RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
		})
		require.NoError(s.t, err)
		EnvWaitFor(s.t, env, 3*time.Minute, reason, func() bool {
			return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_PAUSED
		})
	}

	// add: the publication owner adds the table first, then the mirror picks it up
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pubName, added)))
	pause(env, "wait for pause for add table")
	_, err = s.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					AdditionalTables: []*protos.TableMapping{
						{SourceTableIdentifier: added, DestinationTableIdentifier: "nonowner_added"},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table addition to finish", func() bool {
		valid, err := s.checkCatalogTableMapping(ctx, flowConnConfig.FlowJobName, []string{added, original})
		return err == nil && valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	RequireEqualTables(s.ch, "nonowner_added", cols)
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id, val) values (2,'cdc_after_add')", added)))
	EnvWaitForEqualTables(env, s.ch, "cdc after add table", "nonowner_added", cols)

	// remove: the mirror stops reading the table and leaves the publication untouched
	pause(env, "wait for pause for remove table")
	_, err = s.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					RemovedTables: []*protos.TableMapping{
						{SourceTableIdentifier: original, DestinationTableIdentifier: "nonowner_original"},
					},
				},
			},
		},
	})
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for table removal to finish", func() bool {
		valid, err := s.checkCatalogTableMapping(ctx, flowConnConfig.FlowJobName, []string{added})
		return err == nil && valid && env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	var originalStillPublished bool
	require.NoError(s.t, s.pg.Conn().QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
		pubName, Schema(s), "nonowner_original").Scan(&originalStillPublished))
	require.True(s.t, originalStillPublished, "removing a table should not alter a customer-managed publication")
	s.waitForActiveSlotForPostgresMirror(env, flowConnConfig.FlowJobName)

	// resync: validation, teardown and setup must all succeed without publication ownership
	pause(env, "wait for pause before resync")
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id, val) values (3,'resync')", added)))
	_, err = s.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
		DropMirrorStats:    true,
	})
	require.NoError(s.t, err)
	newEnv, err := GetPeerflow(ctx, s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	EnvWaitForEqualTables(newEnv, s.ch, "resync", "nonowner_added", cols)
	EnvWaitFor(s.t, newEnv, 3*time.Minute, "wait for resynced mirror to be in cdc", func() bool {
		return newEnv.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})
	require.NoError(s.t, s.source.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id, val) values (4,'cdc_after_resync')", added)))
	EnvWaitForEqualTables(newEnv, s.ch, "cdc after resync", "nonowner_added", cols)
	requirePublicationOwnedByAdmin()

	// drop: the slot goes away, the customer-managed publication stays
	_, err = s.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
	})
	require.NoError(s.t, err)
	s.waitForFlowDropped(newEnv, flowConnConfig.FlowJobName)

	var pubExists bool
	require.NoError(s.t, s.pg.Conn().QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", pubName).Scan(&pubExists))
	require.True(s.t, pubExists, "dropping the mirror must not drop a customer-managed publication")

	var slotName string
	err = s.pg.Conn().QueryRow(ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
		"peerflow_slot_"+flowConnConfig.FlowJobName).Scan(&slotName)
	require.ErrorIs(s.t, err, pgx.ErrNoRows, "dropping the mirror must drop its replication slot, got %v", err)
}
