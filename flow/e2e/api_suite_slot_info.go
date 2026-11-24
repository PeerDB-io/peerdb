package e2e

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func slotNames(slots []*protos.SlotInfo) []string {
	names := make([]string, len(slots))
	for i, slot := range slots {
		names[i] = slot.SlotName
	}
	return names
}

func (s APITestSuite) TestGetSlotInfoForFlow() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only for PostgreSQL")
	}
	ctx := s.t.Context()

	createSlot := func(slotName string) {
		s.t.Helper()
		_, err := s.pg.Conn().Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName)
		require.NoError(s.t, err)
		s.t.Cleanup(func() {
			_, _ = s.pg.Conn().Exec(context.Background(),
				"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slotName)
		})
	}
	// stands in for the slot of another mirror reading from the same database
	foreignSlot := connpostgres.GetDefaultSlotName("slotinfo_foreign_" + s.suffix)
	createSlot(foreignSlot)
	customSlot := "slotinfo_custom_" + s.suffix
	customPublication := "slotinfo_pub_" + s.suffix

	tc := NewTemporalClient(s.t)
	startFlow := func(name string, table string, withCustomSlot bool) *protos.FlowConnectionConfigs {
		s.t.Helper()
		require.NoError(s.t, s.source.Exec(ctx,
			fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, table))))
		connectionGen := FlowConnectionGenerationConfig{
			FlowJobName:      name,
			TableNameMapping: map[string]string{AttachSchema(s, table): table},
			Destination:      s.ch.Peer().Name,
		}
		flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
		if withCustomSlot {
			// the publication must exist before the slot, pgoutput fails on changes decoded from before it
			require.NoError(s.t, s.source.Exec(ctx,
				fmt.Sprintf(`CREATE PUBLICATION "%s" FOR TABLE %s`, customPublication, AttachSchema(s, table))))
			s.t.Cleanup(func() {
				_ = s.source.Exec(context.Background(), fmt.Sprintf(`DROP PUBLICATION IF EXISTS "%s"`, customPublication))
			})
			createSlot(customSlot)
			flowConnConfig.DoInitialSnapshot = false
			flowConnConfig.ReplicationSlotName = customSlot
			flowConnConfig.PublicationName = customPublication
		}
		_, err := s.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
		require.NoError(s.t, err)
		env, err := GetPeerflow(ctx, s.catalog, tc, name)
		require.NoError(s.t, err)
		SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
		s.t.Cleanup(func() {
			_, _ = s.FlowStateChange(context.Background(), &protos.FlowStateChangeRequest{
				FlowJobName:        name,
				RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
			})
		})
		return flowConnConfig
	}
	defaultFlow := startFlow("slotinfo_default_"+s.suffix, "slotinfo_default", false)
	customFlow := startFlow("slotinfo_custom_"+s.suffix, "slotinfo_custom", true)
	defaultSlot := connpostgres.GetDefaultSlotName(defaultFlow.FlowJobName)
	peerName := defaultFlow.SourceName

	require.Eventually(s.t, func() bool {
		res, err := s.GetSlotInfo(ctx, &protos.PostgresPeerActivityInfoRequest{PeerName: peerName, PeerdbManagedOnly: true})
		if err != nil {
			s.t.Log(err)
			return false
		}
		names := slotNames(res.SlotData)
		return slices.Contains(names, defaultSlot) && slices.Contains(names, foreignSlot) && slices.Contains(names, customSlot)
	}, 3*time.Minute, time.Second, "peer-wide listing should include every PeerDB slot in the database")

	res, err := s.GetSlotInfo(ctx, &protos.PostgresPeerActivityInfoRequest{
		PeerName: peerName, PeerdbManagedOnly: true, FlowJobName: defaultFlow.FlowJobName,
	})
	require.NoError(s.t, err)
	require.Equal(s.t, []string{defaultSlot}, slotNames(res.SlotData))

	res, err = s.GetSlotInfo(ctx, &protos.PostgresPeerActivityInfoRequest{
		PeerName: peerName, PeerdbManagedOnly: true, FlowJobName: customFlow.FlowJobName,
	})
	require.NoError(s.t, err)
	require.Equal(s.t, []string{customSlot}, slotNames(res.SlotData))

	_, err = s.GetSlotInfo(ctx, &protos.PostgresPeerActivityInfoRequest{
		PeerName: peerName, FlowJobName: "slotinfo_missing_" + s.suffix,
	})
	require.Equal(s.t, codes.NotFound, status.Code(err), err)

	_, err = s.GetSlotInfo(ctx, &protos.PostgresPeerActivityInfoRequest{
		PeerName: s.ch.Peer().Name, FlowJobName: defaultFlow.FlowJobName,
	})
	require.Equal(s.t, codes.InvalidArgument, status.Code(err), err)
}
