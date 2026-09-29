package common

import (
	"errors"
	"net"
	"strings"
)

// CloudSQLIAMTLSConfig holds the connection settings that decide whether a Cloud SQL IAM login token
// can be sent safely as the database password.
type CloudSQLIAMTLSConfig struct {
	Host                 string
	TlsHost              string
	RootCa               string
	DisableTls           bool
	SkipCertVerification bool
}

// Verify checks that the connection settings are safe for sending an IAM token as the password.
// The server certificate's hostname must be verified: without it any certificate from a shared Cloud SQL CA
// would be accepted, handing that server a reusable login token. TlsHost must be the instance DNS name,
// unless Host is already a DNS name, which then is the name verified.
func (c CloudSQLIAMTLSConfig) Verify() error {
	if c.DisableTls {
		return errors.New("TLS is required")
	}
	if c.SkipCertVerification {
		return errors.New("certificate verification cannot be skipped")
	}
	host := strings.Trim(strings.TrimSpace(c.Host), "[]")
	// a hostname Host is itself verified against the server certificate, but an IP has no name to check
	if strings.TrimSpace(c.TlsHost) == "" && (host == "" || net.ParseIP(host) != nil) {
		return errors.New("TLS host must be set to the instance DNS name unless host is a DNS name")
	}
	// Cloud SQL server certs chain to Google-managed private CA
	// (per-instance CA, or regional shared "Google Cloud SQL Server CA"). Never public.
	if strings.TrimSpace(c.RootCa) == "" {
		return errors.New("root CA is required")
	}
	return nil
}
