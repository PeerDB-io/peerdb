package utils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
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
)

// cloudSQLPSCUnavailable reports whether a failed WithPSC dial means this deployment has no Private Service Connect
// path to the instance, so the dial should fall back to the instance IP. Any other failure is returned as is:
// falling back on it could move traffic that is meant to stay private onto the public IP.
func cloudSQLPSCUnavailable(err error) bool {
	// the instance has no PSC DNS name. Also returned for a bad instance name,
	// which the IP dial fails on again, so falling back still surfaces the right error
	if _, ok := errors.AsType[*errtype.ConfigError](err); ok {
		return true
	}
	// the PSC DNS name does not resolve: nothing publishes a record for it here, so there is no PSC endpoint.
	// Timeouts and other DNS failures are not proof of that and are returned for a retry
	if _, ok := errors.AsType[*errtype.DialError](err); ok {
		if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
			return dnsErr.IsNotFound
		}
	}
	return false
}

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
	// the dialer outlives the request that happened to create it
	dialer, err := cloudsqlconn.NewDialer(
		context.WithoutCancel(ctx),
		cloudsqlconn.WithIAMAuthN(),
		cloudsqlconn.WithIAMAuthNCredentials(apiCredentials, loginCredentials),
		// lets the host be a DNS name with a TXT record pointing to the instance connection name
		cloudsqlconn.WithDNSResolver(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud SQL dialer: %w", err)
	}
	sharedCloudSQLDialer.dialer = dialer
	return dialer, nil
}

// cloudSQLIAMServiceAccount returns the email of the service account this deployment impersonates,
// for deriving the database user when none is configured.
func cloudSQLIAMServiceAccount() (string, error) {
	serviceAccount := envValue(workloadIdentityServiceAccountEnv)
	if serviceAccount == "" {
		return "", exceptions.NewCloudSQLIAMAuthError(fmt.Errorf(
			"user is not set and cannot be derived without deployment environment variable %s",
			workloadIdentityServiceAccountEnv,
		))
	}
	return serviceAccount, nil
}

// CloudSQLPostgresIAMUser returns the Postgres database user of the service account this deployment impersonates:
// Cloud SQL names Postgres IAM service account users after the service account email without ".gserviceaccount.com".
func CloudSQLPostgresIAMUser() (string, error) {
	serviceAccount, err := cloudSQLIAMServiceAccount()
	if err != nil {
		return "", err
	}
	user, found := strings.CutSuffix(serviceAccount, ".gserviceaccount.com")
	if !found {
		return "", exceptions.NewCloudSQLIAMAuthError(fmt.Errorf(
			"user is not set and service account %q is not a .gserviceaccount.com email", serviceAccount))
	}
	return user, nil
}

// CloudSQLMySQLIAMUser returns the MySQL database user of the service account this deployment impersonates:
// Cloud SQL names MySQL IAM service account users after the part of the email before "@".
func CloudSQLMySQLIAMUser() (string, error) {
	serviceAccount, err := cloudSQLIAMServiceAccount()
	if err != nil {
		return "", err
	}
	user, domain, found := strings.Cut(serviceAccount, "@")
	if !found || user == "" || !strings.HasSuffix(domain, ".gserviceaccount.com") {
		return "", exceptions.NewCloudSQLIAMAuthError(fmt.Errorf(
			"user is not set and service account %q is not a .gserviceaccount.com email", serviceAccount))
	}
	return user, nil
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
	var tunnelOpts []cloudsqlconn.DialOption
	if d.Tunnel.IsActive() {
		tunnelOpts = append(tunnelOpts, cloudsqlconn.WithOneOffDialFunc(d.Tunnel.DialContext))
	}
	instance := strings.TrimSpace(d.Instance)
	// Private Service Connect first: the instance's PSC DNS name only resolves where a PSC endpoint for it is set up
	// (in ClickPipes, the DNS proxy record of a reverse private endpoint), so one process can reach PSC and
	// non-PSC instances without being told which is which
	conn, err := dialer.Dial(ctx, instance, append(slices.Clip(tunnelOpts), cloudsqlconn.WithPSC())...)
	if err != nil && cloudSQLPSCUnavailable(err) {
		// public IP if the instance has one, otherwise private IP
		conn, err = dialer.Dial(ctx, instance, append(slices.Clip(tunnelOpts), cloudsqlconn.WithAutoIP())...)
	}
	if err != nil {
		// a bad instance name will not fix itself on retry, unlike a failure to reach the instance.
		// RefreshError is returned as is: cloudsqlconn documents it as usually retryable (Admin API failures)
		if _, ok := errors.AsType[*errtype.ConfigError](err); ok {
			return nil, exceptions.NewCloudSQLIAMAuthError(err)
		}
		return nil, err
	}
	return conn, nil
}
