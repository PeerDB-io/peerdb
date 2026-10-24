package e2e

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestSwitchboardDSN(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options map[string]string
		want    []string
	}{
		{name: "no options"},
		{
			name:    "guardrails",
			options: map[string]string{"max_rows": "100", "max_bytes": "1000"},
			want:    []string{"-c", "peerdb.max_rows=100", "-c", "peerdb.max_bytes=1000"},
		},
		{
			name:    "special characters",
			options: map[string]string{"tag": "a+b&c=d"},
			want:    []string{"-c", "peerdb.tag=a+b&c=d"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := pgx.ParseConfig(SwitchboardDSN("test_peer", tc.options))
			require.NoError(t, err)
			require.Equal(t, "test_peer", config.Database)
			require.ElementsMatch(t, tc.want, strings.Fields(config.RuntimeParams["options"]))
		})
	}
}
