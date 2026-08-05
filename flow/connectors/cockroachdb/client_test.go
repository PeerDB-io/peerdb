package conncockroachdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestGetCRDBConnectionString(t *testing.T) {
	config := &protos.CockroachDBConfig{
		Host:     "localhost",
		Port:     26257,
		User:     "root",
		Password: "",
		Database: "defaultdb",
	}
	assert.Equal(t,
		"postgres://root@localhost:26257/defaultdb?application_name=peerdb&client_encoding=UTF8",
		GetCRDBConnectionString(config, ""))

	config.Password = "p@ss/word"
	config.RequireTls = true
	assert.Equal(t,
		"postgres://root@localhost:26257/defaultdb?application_name=peerdb_myflow&client_encoding=UTF8&sslmode=require",
		GetCRDBConnectionString(config, "myflow"))
}

func TestClassifyConnectError(t *testing.T) {
	for _, code := range []string{
		pgerrcode.InvalidAuthorizationSpecification,
		pgerrcode.InvalidPassword,
		pgerrcode.InsufficientPrivilege,
	} {
		err := classifyConnectError(fmt.Errorf("failed to connect: %w",
			&pgconn.PgError{Code: code, Message: "password authentication failed for user root"}))
		var authErr *exceptions.AuthError
		require.ErrorAs(t, err, &authErr, "SQLSTATE %s should classify as AuthError", code)
	}

	// other pg errors and non-pg errors pass through unchanged
	serialization := fmt.Errorf("connect: %w", &pgconn.PgError{Code: pgerrcode.SerializationFailure})
	require.Equal(t, serialization, classifyConnectError(serialization))
	network := errors.New("dial tcp: connection refused")
	require.Equal(t, network, classifyConnectError(network))
	require.NoError(t, classifyConnectError(nil))
}

func TestParseCrdbMajorVersion(t *testing.T) {
	major, err := parseCrdbMajorVersion("CockroachDB CCL v25.4.13 (x86_64-pc-linux-gnu, built 2026/07/10)")
	require.NoError(t, err)
	assert.Equal(t, 25, major)

	major, err = parseCrdbMajorVersion("CockroachDB v23.1.0 (aarch64-unknown-linux-gnu)")
	require.NoError(t, err)
	assert.Equal(t, 23, major)

	_, err = parseCrdbMajorVersion("PostgreSQL 16.2 on x86_64-pc-linux-gnu")
	require.Error(t, err)
}
