package peerflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestApplyEnvUpdate(t *testing.T) {
	t.Parallel()

	t.Run("merge into nil env", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(nil, map[string]string{"A": "1"}, nil)
		require.Equal(t, map[string]string{"A": "1"}, got)
	})

	t.Run("merge keeps untouched keys", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(map[string]string{"A": "1", "B": "2"}, map[string]string{"B": "3", "C": "4"}, nil)
		require.Equal(t, map[string]string{"A": "1", "B": "3", "C": "4"}, got)
	})

	t.Run("empty update is a no-op", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(map[string]string{"A": "1"}, map[string]string{}, nil)
		require.Equal(t, map[string]string{"A": "1"}, got)
	})

	t.Run("remove deletes keys and ignores unknown ones", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(map[string]string{"A": "1", "B": "2"}, nil, []string{"A", "missing"})
		require.Equal(t, map[string]string{"B": "2"}, got)
	})

	t.Run("update and remove together", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(map[string]string{"K1": "V1", "K2": "V2"}, map[string]string{"K1": "k1b"}, []string{"K2"})
		require.Equal(t, map[string]string{"K1": "k1b"}, got)
	})

	t.Run("remove wins over update for same key", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(map[string]string{"A": "1"}, map[string]string{"A": "2"}, []string{"A"})
		require.Empty(t, got)
	})

	t.Run("remove on nil env is a no-op", func(t *testing.T) {
		t.Parallel()
		got := applyEnvUpdate(nil, nil, []string{"A"})
		require.Nil(t, got)
	})
}

func TestApplyQueryCDCConfigUpdate(t *testing.T) {
	t.Parallel()

	old := &protos.QueryCdcConfig{PullSyncParallelism: 3, SafetyLagSeconds: 30, MaxQueryWindowSeconds: 300}
	newConfig := &protos.QueryCdcConfig{PullSyncParallelism: 5, SafetyLagSeconds: 0, MaxQueryWindowSeconds: 600}

	t.Run("absent update preserves settings", func(t *testing.T) {
		t.Parallel()
		cfg := &protos.FlowConnectionConfigsCore{SourceConnectorConfig: &protos.FlowConnectionConfigsCore_BigqueryCdcConfig{
			BigqueryCdcConfig: &protos.BigqueryCdcConfig{QueryCdc: old},
		}}
		applyQueryCDCConfigUpdate(cfg, nil)
		require.Same(t, old, cfg.GetBigqueryCdcConfig().GetQueryCdc())
	})

	t.Run("present update replaces all settings including zero", func(t *testing.T) {
		t.Parallel()
		cfg := &protos.FlowConnectionConfigsCore{SourceConnectorConfig: &protos.FlowConnectionConfigsCore_BigqueryCdcConfig{
			BigqueryCdcConfig: &protos.BigqueryCdcConfig{
				ReplicationMethod: protos.BigQueryReplicationMethod_BIGQUERY_REPLICATION_METHOD_QUERY,
				QueryCdc:          old,
			},
		}}
		applyQueryCDCConfigUpdate(cfg, newConfig)
		require.Equal(t, newConfig, cfg.GetBigqueryCdcConfig().GetQueryCdc())
		require.NotSame(t, newConfig, cfg.GetBigqueryCdcConfig().GetQueryCdc())
		require.Equal(t, protos.BigQueryReplicationMethod_BIGQUERY_REPLICATION_METHOD_QUERY, cfg.GetBigqueryCdcConfig().GetReplicationMethod())
		require.Equal(t, int32(30), old.SafetyLagSeconds)
	})

	t.Run("other source ignores query CDC settings", func(t *testing.T) {
		t.Parallel()
		cfg := &protos.FlowConnectionConfigsCore{}
		applyQueryCDCConfigUpdate(cfg, newConfig)
		require.Nil(t, cfg.GetBigqueryCdcConfig())
	})
}
