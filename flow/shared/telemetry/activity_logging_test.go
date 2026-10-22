package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactURIUserInfo(t *testing.T) {
	for uri, expected := range map[string]string{ //nolint:gosec // fake credentials
		"mongodb://user:secret@host1:27017,host2:27017/db?replicaSet=rs0": "mongodb://host1:27017,host2:27017/db?replicaSet=rs0",
		"mongodb+srv://user:secret@cluster.example.com/":                  "mongodb+srv://cluster.example.com/",
		"mongodb://host:27017": "mongodb://host:27017",
		"mongodb://%zz@host":   "",
	} {
		require.Equal(t, expected, redactURIUserInfo(uri), uri)
	}
}
