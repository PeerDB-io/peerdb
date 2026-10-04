package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestCloudSQLDialerVerifyAuthConfig(t *testing.T) {
	for _, test := range []struct {
		instance string
		valid    bool
	}{
		{instance: "project:us-central1:instance", valid: true},
		{instance: "example.com:project:us-central1:instance", valid: true},
		{instance: " project:us-central1:instance ", valid: true},
		{instance: "db.internal.example.com", valid: true}, // DNS name, resolved through a TXT record
		{instance: ""},
		{instance: " "},
		{instance: "project:instance"},
		{instance: "35.238.144.132"},
		{instance: "[2001:db8::1]"},
	} {
		t.Run(test.instance, func(t *testing.T) {
			err := (&CloudSQLDialer{Instance: test.instance}).VerifyAuthConfig()
			if test.valid {
				require.NoError(t, err)
				return
			}
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}

func TestCloudSQLIPTypeFromEnv(t *testing.T) {
	for _, value := range []string{"", "public", "PRIVATE", "psc", "auto"} {
		t.Setenv(CloudSQLIPTypeEnv, value)
		_, err := cloudSQLIPTypeFromEnv()
		require.NoError(t, err, value)
	}
	t.Setenv(CloudSQLIPTypeEnv, "bogus")
	_, err := cloudSQLIPTypeFromEnv()
	require.Error(t, err)
}

func TestCloudSQLPostgresIAMUser(t *testing.T) {
	t.Run("strips the service account suffix", func(t *testing.T) {
		t.Setenv(workloadIdentityServiceAccountEnv, "ch-deadbeef@tenant-project.iam.gserviceaccount.com")
		user, err := CloudSQLPostgresIAMUser()
		require.NoError(t, err)
		require.Equal(t, "ch-deadbeef@tenant-project.iam", user)
	})

	t.Run("missing service account", func(t *testing.T) {
		t.Setenv(workloadIdentityServiceAccountEnv, "")
		_, err := CloudSQLPostgresIAMUser()
		var authErr *exceptions.CloudSQLIAMAuthError
		require.ErrorAs(t, err, &authErr)
	})

	t.Run("not a service account email", func(t *testing.T) {
		t.Setenv(workloadIdentityServiceAccountEnv, "someone@example.com")
		_, err := CloudSQLPostgresIAMUser()
		var authErr *exceptions.CloudSQLIAMAuthError
		require.ErrorAs(t, err, &authErr)
	})
}

func TestCloudSQLMySQLIAMUser(t *testing.T) {
	t.Run("keeps the part before the at sign", func(t *testing.T) {
		t.Setenv(workloadIdentityServiceAccountEnv, "ch-deadbeef@tenant-project.iam.gserviceaccount.com")
		user, err := CloudSQLMySQLIAMUser()
		require.NoError(t, err)
		require.Equal(t, "ch-deadbeef", user)
	})

	for name, serviceAccount := range map[string]string{
		"missing service account":     "",
		"not a service account email": "someone@example.com",
		"no local part":               "@tenant-project.iam.gserviceaccount.com",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(workloadIdentityServiceAccountEnv, serviceAccount)
			_, err := CloudSQLMySQLIAMUser()
			var authErr *exceptions.CloudSQLIAMAuthError
			require.ErrorAs(t, err, &authErr)
		})
	}
}
