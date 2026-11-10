package utils

import (
	"errors"
	"net"
	"testing"

	"cloud.google.com/go/cloudsqlconn/errtype"
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

func TestCloudSQLPSCUnavailable(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "instance without PSC",
			err:  errtype.NewConfigError(`instance does not have IP of type "PSC"`, "project:region:instance"),
			want: true,
		},
		{
			name: "PSC DNS name not found",
			err: errtype.NewDialError("failed to dial", "project:region:instance",
				&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "x.sql.goog", IsNotFound: true}}),
			want: true,
		},
		{
			name: "PSC DNS lookup timeout",
			err: errtype.NewDialError("failed to dial", "project:region:instance",
				&net.OpError{Op: "dial", Err: &net.DNSError{Err: "i/o timeout", Name: "x.sql.goog", IsTimeout: true}}),
		},
		{
			name: "PSC endpoint unreachable",
			err: errtype.NewDialError("failed to dial", "project:region:instance",
				&net.OpError{Op: "dial", Err: errors.New("connection refused")}),
		},
		{
			name: "Admin API failure",
			err:  errtype.NewRefreshError("failed to get instance metadata", "project:region:instance", errors.New("quota exceeded")),
		},
		{
			name: "unrelated error",
			err:  errors.New("boom"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cloudSQLPSCUnavailable(test.err))
		})
	}
}
