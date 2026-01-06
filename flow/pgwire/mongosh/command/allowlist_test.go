package command

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
			err := ValidateCommand(tt.cmd)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateInput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:  "valid single statement",
			input: `db.users.find({})`,
		},
		{
			name:  "trailing semicolon allowed",
			input: `db.users.find({});`,
		},
		{
			name:    "multiple statements not allowed",
			input:   `db.users.find({}); db.orders.find({})`,
			wantErr: true,
		},
		{
			name:  "semicolon in string is ok",
			input: `db.users.find({"name": "test;value"})`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateInput(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckMultipleStatements(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:  "single statement",
			input: `db.users.find({})`,
		},
		{
			name:  "trailing semicolon ok",
			input: `db.users.find({});`,
		},
		{
			name:  "trailing semicolon with whitespace ok",
			input: `db.users.find({});  `,
		},
		{
			name:    "multiple statements",
			input:   `db.users.find({}); db.orders.find({})`,
			wantErr: true,
		},
		{
			name:  "semicolon in string",
			input: `db.users.find({"name": "test;value"})`,
		},
		{
			name:  "semicolon in single quotes",
			input: `db.users.find({'name': 'test;value'})`,
		},
		{
			name:  "escaped quote in string",
			input: `db.users.find({"name": "test\";value"})`,
		},
		{
			name:    "semicolon outside string",
			input:   `db.users.find({"name": "test"}) ; db.orders.find({})`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkMultipleStatements(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsCommandAllowed(t *testing.T) {
	allowed := []string{"find", "aggregate", "ping", "hello"}
	for _, cmd := range allowed {
		if !IsCommandAllowed(cmd) {
			t.Errorf("expected %q to be allowed", cmd)
		}
	}

	denied := []string{"insert", "update", "delete", "drop", "shutdown", "FIND", "Aggregate"}
	for _, cmd := range denied {
		if IsCommandAllowed(cmd) {
			t.Errorf("expected %q to be denied", cmd)
		}
	}
}

func TestAllowedCommandsStructure(t *testing.T) {
	// Verify commands with required args
	hasRequired := map[string]int{
		"aggregate": 1, "distinct": 1, "dataSize": 1,
	}
	// Verify commands with optional args
	hasOptional := map[string]struct{}{
		"find": {}, "aggregate": {}, "collStats": {}, "dbStats": {},
	}
	// Verify commands with no args
	noArgs := map[string]struct{}{
		"ping": {}, "buildInfo": {}, "hostInfo": {},
	}

	for _, cmds := range AllowedCommands {
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
