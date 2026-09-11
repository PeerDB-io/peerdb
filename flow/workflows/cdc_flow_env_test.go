package peerflow

import (
	"testing"

	"github.com/stretchr/testify/require"
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
