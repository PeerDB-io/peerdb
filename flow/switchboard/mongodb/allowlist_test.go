package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestValidateCommand(t *testing.T) {
	tests := []struct {
		name    string
		cmd     bson.D
		wantErr bool
	}{
		{
			name: "allowed find command",
			cmd: bson.D{
				{Key: "find", Value: "users"},
				{Key: "filter", Value: bson.D{}},
			},
		},
		{
			name: "allowed aggregate command",
			cmd: bson.D{
				{Key: "aggregate", Value: "orders"},
				{Key: "pipeline", Value: bson.A{}},
			},
		},
		{
			name: "denied insert command",
			cmd: bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{}},
			},
			wantErr: true,
		},
		{
			name: "denied drop command",
			cmd: bson.D{
				{Key: "drop", Value: "users"},
			},
			wantErr: true,
		},
		{
			name:    "empty command",
			cmd:     bson.D{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCommand(tt.cmd)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsCommandAllowed(t *testing.T) {
	allowed := []string{"find", "aggregate", "ping", "hello", "FIND", "Aggregate"}
	for _, cmd := range allowed {
		if !isCommandAllowed(cmd) {
			t.Errorf("expected %q to be allowed", cmd)
		}
	}

	denied := []string{"insert", "update", "delete", "drop", "shutdown"}
	for _, cmd := range denied {
		if isCommandAllowed(cmd) {
			t.Errorf("expected %q to be denied", cmd)
		}
	}
}

func TestLookupCommand(t *testing.T) {
	cmd, ok := lookupCommand("find")
	require.True(t, ok)
	require.True(t, cmd.ReturnsCursor)
	require.False(t, cmd.AdminDB)

	cmd, ok = lookupCommand("listDatabases")
	require.True(t, ok)
	require.False(t, cmd.ReturnsCursor)
	require.True(t, cmd.AdminDB)

	cmd, ok = lookupCommand("aggregate")
	require.True(t, ok)
	require.True(t, cmd.ReturnsCursor)

	cmd, ok = lookupCommand("ping")
	require.True(t, ok)
	require.False(t, cmd.ReturnsCursor)
	require.False(t, cmd.AdminDB)

	_, ok = lookupCommand("insert")
	require.False(t, ok)
}

func TestAllowedCommandsStructure(t *testing.T) {
	hasRequired := map[string]int{
		"aggregate": 1, "distinct": 1, "datasize": 1,
	}
	hasOptional := map[string]struct{}{
		"find": {}, "aggregate": {}, "collstats": {}, "dbstats": {},
	}
	noArgs := map[string]struct{}{
		"ping": {}, "buildinfo": {}, "hostinfo": {},
	}

	for _, cmds := range allowedCommands {
		for _, cmd := range cmds {
			if count, ok := hasRequired[cmd.Name]; ok && len(cmd.Required) != count {
				t.Errorf("%s: expected %d required args, got %d", cmd.Name, count, len(cmd.Required))
			}
			if _, ok := hasOptional[cmd.Name]; ok && len(cmd.Optional) == 0 {
				t.Errorf("%s: expected optional args but got none", cmd.Name)
			}
			if _, ok := noArgs[cmd.Name]; ok && (len(cmd.Required) > 0 || len(cmd.Optional) > 0) {
				t.Errorf("%s: expected no args", cmd.Name)
			}
		}
	}
}
