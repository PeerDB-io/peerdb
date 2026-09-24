package e2e

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
)

// createPinnedServerIdPeer creates a MySQL peer that clones the suite source's config while
// pinning the given server_id.
func createPinnedServerIdPeer(s APITestSuite, mySource *MySqlSource, name string, serverID uint32) {
	s.t.Helper()
	pinnedConfig := proto.CloneOf(mySource.Config)
	pinnedConfig.ServerId = &serverID
	_, err := s.CreatePeer(s.t.Context(), &protos.CreatePeerRequest{
		Peer: &protos.Peer{
			Name:   name,
			Type:   protos.DBType_MYSQL,
			Config: &protos.Peer_MysqlConfig{MysqlConfig: pinnedConfig},
		},
	})
	require.NoError(s.t, err)
}

// cleanupMirrorAndPeers tears down the mirror and then the given peers via gRPC, even if the test
// fails, so nothing leaks. t.Context() is already canceled when cleanups run, so it uses a fresh
// context.
func cleanupMirrorAndPeers(s APITestSuite, flowJobName string, peerNames ...string) {
	s.t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if _, err := s.FlowStateChange(cleanupCtx, &protos.FlowStateChangeRequest{
			FlowJobName:        flowJobName,
			RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
		}); err != nil {
			s.t.Logf("failed to terminate mirror %s: %v", flowJobName, err)
			return
		}
		for {
			var workflowID string
			err := s.catalog.QueryRow(
				cleanupCtx, "select workflow_id from flows where name = $1", flowJobName,
			).Scan(&workflowID)
			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			select {
			case <-cleanupCtx.Done():
				s.t.Logf("timed out waiting for mirror %s to be dropped", flowJobName)
				return
			case <-time.After(5 * time.Second):
			}
		}
		for _, peerName := range peerNames {
			if _, err := s.DropPeer(cleanupCtx, &protos.DropPeerRequest{PeerName: peerName}); err != nil {
				s.t.Logf("failed to drop peer %s: %v", peerName, err)
			}
		}
	})
}

// waitForReplicaWithServerId waits until the source lists a replica registered with serverID,
// i.e. a mirror pinning it started streaming the binlog.
func waitForReplicaWithServerId(s APITestSuite, mySource *MySqlSource, serverID uint32) {
	s.t.Helper()
	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		// Conn() is non-nil here: the suite source has already executed queries by now.
		found, err := mysql_validation.HasReplicaWithServerId(mySource.Conn(), serverID)
		assert.NoError(c, err)
		assert.True(c, found, "source does not list a replica registered with server_id %d", serverID)
	}, 3*time.Minute, time.Second)
}

// TestValidateCDCMirror_ServerIDResync verifies that a running CDC mirror whose MySQL source peer
// pins a fixed server_id can be resynced. Resync validates the mirror before dropping it, while
// the mirror's own binlog connection is still registered on the source with the pinned server_id;
// validation must not mistake that registration for a foreign replica.
func (s APITestSuite) TestValidateCDCMirror_ServerIDResync() {
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("server_id validation is MySQL-specific")
	}
	ctx := s.t.Context()

	tableName := "serverid_resync"
	require.NoError(s.t, s.source.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	require.NoError(s.t, s.source.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))

	serverID := uint32(4321)
	peerName := "serverid_resync_" + s.suffix
	createPinnedServerIdPeer(s, mySource, peerName, serverID)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "serverid_resync_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SourceName = peerName
	flowConnConfig.DoInitialSnapshot = true

	createResp, err := s.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, createResp)
	cleanupMirrorAndPeers(s, flowConnConfig.FlowJobName, peerName)

	// Wait until the mirror streams the binlog: its replica registration with the pinned
	// server_id must be visible on the source.
	waitForReplicaWithServerId(s, mySource, serverID)

	// Resync validates the mirror while its own replica registration is still present on the
	// source; it must not be rejected because of it.
	_, err = s.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)
}

// TestValidateCDCMirror_ServerIDInUseAcrossPeers verifies that a CDC mirror is rejected when its
// MySQL source peer pins a server_id that is already registered by another replica on the source
// database — here, a mirror streaming through a DIFFERENT peer pinning the same server_id. The
// cross-mirror peer-reuse check cannot relate the two peers, so only the replica registered on
// the source betrays the collision.
func (s APITestSuite) TestValidateCDCMirror_ServerIDInUseAcrossPeers() {
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("server_id validation is MySQL-specific")
	}
	ctx := s.t.Context()

	tableName := "serverid_crosspeer"
	require.NoError(s.t, s.source.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	require.NoError(s.t, s.source.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))

	// Two distinct peers pointing at the same source instance, pinning the same server_id.
	serverID := uint32(4422)
	occupantPeer := "serverid_crosspeer_a_" + s.suffix
	contenderPeer := "serverid_crosspeer_b_" + s.suffix
	createPinnedServerIdPeer(s, mySource, occupantPeer, serverID)
	createPinnedServerIdPeer(s, mySource, contenderPeer, serverID)

	// A streaming CDC mirror on the first peer registers a replica with the pinned server_id.
	existingGen := FlowConnectionGenerationConfig{
		FlowJobName:      "serverid_crosspeer_existing_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	existingCfg := existingGen.GenerateFlowConnectionConfigs(s)
	existingCfg.SourceName = occupantPeer
	existingCfg.DoInitialSnapshot = true
	createResp, err := s.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{ConnectionConfigs: existingCfg})
	require.NoError(s.t, err)
	require.NotNil(s.t, createResp)
	cleanupMirrorAndPeers(s, existingCfg.FlowJobName, occupantPeer, contenderPeer)

	waitForReplicaWithServerId(s, mySource, serverID)

	// Validating a new mirror on the second peer must fail with the server_id collision. The
	// mirror's binlog connection is re-registered on each sync batch, so the replica listing can
	// blink between batches; retry to absorb that.
	newGen := FlowConnectionGenerationConfig{
		FlowJobName:      "serverid_crosspeer_new_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	newCfg := newGen.GenerateFlowConnectionConfigs(s)
	newCfg.SourceName = contenderPeer
	newCfg.DoInitialSnapshot = true

	require.EventuallyWithT(s.t, func(c *assert.CollectT) {
		res, err := s.ValidateCDCMirror(ctx, &protos.CreateCDCFlowRequest{ConnectionConfigs: newCfg})
		if !assert.Error(c, err) {
			return
		}
		assert.Nil(c, res)
		st, ok := status.FromError(err)
		if !assert.True(c, ok, "expected a gRPC status error, got %v", err) {
			return
		}
		assert.Equal(c, codes.FailedPrecondition, st.Code())
		assert.Contains(c, st.Message(), fmt.Sprintf("pins a replica id = %d", serverID))
		assert.Contains(c, st.Message(), "already in use by a replica registered on the source database")
	}, time.Minute, 2*time.Second)
}
