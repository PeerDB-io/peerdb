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
	// scopes for Cloud SQL Admin API calls (instance metadata, ephemeral certificate)
	gcpCloudSQLAdminScope = "https://www.googleapis.com/auth/sqlservice.admin"
	gcpCloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
	// GCPCloudSQLLoginScope authorizes IAM database logins to Cloud SQL.
	GCPCloudSQLLoginScope = "https://www.googleapis.com/auth/sqlservice.login"
)

// cloudSQLPSCUnavailable reports whether a failed WithPSC dial means there is no PSC path to the instance,
// so the dial should fall back to the instance IP. Other failures must not fall back:
// that could move traffic meant to stay private onto the public IP.
func cloudSQLPSCUnavailable(err error) bool {
	// no PSC DNS name (or a bad instance name, which the IP dial reports again)
	if _, ok := errors.AsType[*errtype.ConfigError](err); ok {
		return true
	}
	// PSC DNS name does not exist. Timeouts and other DNS failures are not proof of that
	if _, ok := errors.AsType[*errtype.DialError](err); ok {
		if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
			return dnsErr.IsNotFound
		}
	}
	return false
}

// The dialer uses deployment-level workload identity and keeps per-instance state internally,
// so one dialer is shared across all mirrors.
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
	// the dialer outlives the request that created it
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

// CloudSQLDialer dials one Cloud SQL instance through the shared dialer.
type CloudSQLDialer struct {
	// Tunnel, when active, carries the connection to the instance.
	Tunnel *SSHTunnel
	// Instance is an instance connection name (project:region:instance) or a DNS name with a TXT record for one.
	Instance string
}

// VerifyAuthConfig checks the instance reference early, so a typo is an auth config error and not a dial error.
func (d *CloudSQLDialer) VerifyAuthConfig() error {
	instance := strings.TrimSpace(d.Instance)
	if instance == "" {
		return exceptions.NewCloudSQLIAMAuthError(
			errors.New("host must be the instance connection name (project:region:instance) or a DNS name for it"))
	}
	// instances are resolved by name, so an IP is a misconfigured host
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

// DialContext matches pgconn.DialFunc. The address is ignored: the dialer resolves the instance IP and port itself.
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
	// try PSC first: its DNS name only resolves where a PSC endpoint is set up,
	// so no config is needed to tell PSC and non-PSC instances apart
	conn, err := dialer.Dial(ctx, instance, append(slices.Clip(tunnelOpts), cloudsqlconn.WithPSC())...)
	if err != nil && cloudSQLPSCUnavailable(err) {
		// public IP if the instance has one, otherwise private IP
		conn, err = dialer.Dial(ctx, instance, append(slices.Clip(tunnelOpts), cloudsqlconn.WithAutoIP())...)
	}
	if err != nil {
		// a bad instance name is not retryable, unlike RefreshError (Admin API failures)
		if _, ok := errors.AsType[*errtype.ConfigError](err); ok {
			return nil, exceptions.NewCloudSQLIAMAuthError(err)
		}
		return nil, err
	}
	return conn, nil
}
