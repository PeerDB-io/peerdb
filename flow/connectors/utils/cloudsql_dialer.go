package utils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/cloudsqlconn"
	"cloud.google.com/go/cloudsqlconn/errtype"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

const (
	// scopes for the Cloud SQL Admin API calls the connector makes (instance metadata, ephemeral certificate)
	gcpCloudSQLAdminScope = "https://www.googleapis.com/auth/sqlservice.admin"
	gcpCloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
	// GCPCloudSQLLoginScope authorizes IAM database logins to Cloud SQL.
	GCPCloudSQLLoginScope = "https://www.googleapis.com/auth/sqlservice.login"

	// CloudSQLIPTypeEnv selects which instance address the connector dials: public (default), private, psc or auto.
	CloudSQLIPTypeEnv = "PEERDB_GCP_CLOUD_SQL_IP_TYPE"
)

// Automatic IAM database authentication: instead of handing a login token to Postgres as the password,
// the Cloud SQL Go connector exchanges the login token for an ephemeral client certificate through the
// Cloud SQL Admin API and opens an mTLS connection to the instance's server-side proxy (port 3307).
// The connector owns token refresh, certificate refresh and server identity verification,
// so none of root CA / TLS host / password handling applies to these connections.
//
// One dialer per process: it caches instance metadata and certificates and refreshes them in the background,
// and creating one per connector would multiply Cloud SQL Admin API calls (which are quota limited).
var sharedCloudSQLDialer struct {
	dialer *cloudsqlconn.Dialer
	lock   sync.Mutex
}

// UseCloudSQLDialer replaces the process-wide dialer, for tests that cannot use workload identity.
func UseCloudSQLDialer(dialer *cloudsqlconn.Dialer) {
	sharedCloudSQLDialer.lock.Lock()
	defer sharedCloudSQLDialer.lock.Unlock()
	sharedCloudSQLDialer.dialer = dialer
}

func getCloudSQLDialer(ctx context.Context) (*cloudsqlconn.Dialer, error) {
	sharedCloudSQLDialer.lock.Lock()
	defer sharedCloudSQLDialer.lock.Unlock()
	if sharedCloudSQLDialer.dialer != nil {
		return sharedCloudSQLDialer.dialer, nil
	}

	apiCredentials, err := NewGCPWorkloadIdentityCredentials(ctx, []string{gcpCloudSQLAdminScope, gcpCloudPlatformScope})
	if err != nil {
		return nil, fmt.Errorf("failed to create API credentials: %w", err)
	}
	loginCredentials, err := NewGCPWorkloadIdentityCredentials(ctx, []string{GCPCloudSQLLoginScope})
	if err != nil {
		return nil, fmt.Errorf("failed to create login credentials: %w", err)
	}
	ipType, err := cloudSQLIPTypeFromEnv()
	if err != nil {
		return nil, err
	}
	// the dialer outlives the request that happened to create it
	dialer, err := cloudsqlconn.NewDialer(
		context.WithoutCancel(ctx),
		cloudsqlconn.WithIAMAuthN(),
		cloudsqlconn.WithIAMAuthNCredentials(apiCredentials, loginCredentials),
		// lets the host be a DNS name with a TXT record pointing to the instance connection name
		cloudsqlconn.WithDNSResolver(),
		cloudsqlconn.WithDefaultDialOptions(ipType),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud SQL dialer: %w", err)
	}
	sharedCloudSQLDialer.dialer = dialer
	return dialer, nil
}

func cloudSQLIPTypeFromEnv() (cloudsqlconn.DialOption, error) {
	switch v := strings.ToLower(os.Getenv(CloudSQLIPTypeEnv)); v {
	case "", "public":
		return cloudsqlconn.WithPublicIP(), nil
	case "private":
		return cloudsqlconn.WithPrivateIP(), nil
	case "psc":
		return cloudsqlconn.WithPSC(), nil
	case "auto":
		return cloudsqlconn.WithAutoIP(), nil
	default:
		return nil, fmt.Errorf("invalid %s %q, expected public, private, psc or auto", CloudSQLIPTypeEnv, v)
	}
}

// CloudSQLDialer dials one Cloud SQL instance through the process-wide connector.
type CloudSQLDialer struct {
	// Tunnel, when active, carries the connection to the instance.
	Tunnel *SSHTunnel
	// Instance is an instance connection name (project:region:instance) or a DNS name with a TXT record for one.
	Instance string
}

// VerifyAuthConfig checks the instance reference early so a typo surfaces as an auth config error
// and not as a connector error on first dial.
func (d *CloudSQLDialer) VerifyAuthConfig() error {
	instance := strings.TrimSpace(d.Instance)
	if instance == "" {
		return exceptions.NewCloudSQLIAMAuthError(
			errors.New("host must be the instance connection name (project:region:instance) or a DNS name for it"))
	}
	// the connector resolves instances by name, an IP address can only be a misconfigured manual-mode host
	if _, err := netip.ParseAddr(strings.Trim(instance, "[]")); err == nil {
		return exceptions.NewCloudSQLIAMAuthError(
			fmt.Errorf("host %q is an IP address, use the instance connection name (project:region:instance)", instance))
	}
	if strings.Contains(instance, ":") && strings.Count(instance, ":") < 2 {
		return exceptions.NewCloudSQLIAMAuthError(
			fmt.Errorf("instance connection name %q must look like project:region:instance", instance))
	}
	return nil
}

// DialContext matches pgconn.DialFunc. Address is ignored: the connector resolves the instance's IP itself
// and always connects to the server-side proxy port, so the configured port has no effect either.
func (d *CloudSQLDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	dialer, err := getCloudSQLDialer(ctx)
	if err != nil {
		return nil, exceptions.NewCloudSQLIAMAuthError(fmt.Errorf("failed to create connector: %w", err))
	}
	var opts []cloudsqlconn.DialOption
	if d.Tunnel.IsActive() {
		opts = append(opts, cloudsqlconn.WithOneOffDialFunc(d.Tunnel.DialContext))
	}
	conn, err := dialer.Dial(ctx, strings.TrimSpace(d.Instance), opts...)
	if err != nil {
		// a bad instance name or a refused Admin API call will not fix itself on retry,
		// unlike a failure to reach the instance
		if _, ok := errors.AsType[*errtype.ConfigError](err); ok {
			return nil, exceptions.NewCloudSQLIAMAuthError(err)
		}
		if _, ok := errors.AsType[*errtype.RefreshError](err); ok {
			return nil, exceptions.NewCloudSQLIAMAuthError(err)
		}
		return nil, err
	}
	return conn, nil
}
