package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloudSQLIAMTLSConfigVerify(t *testing.T) {
	const dnsName = "1-abc.us-central1.sql.goog"
	valid := CloudSQLIAMTLSConfig{Host: "35.238.144.132", TlsHost: dnsName, RootCa: "root-ca"}
	require.NoError(t, valid.Verify())

	for _, test := range []struct {
		name   string
		mutate func(*CloudSQLIAMTLSConfig)
		valid  bool
	}{
		{name: "DNS host without TLS host", mutate: func(c *CloudSQLIAMTLSConfig) { c.Host, c.TlsHost = dnsName, "" }, valid: true},
		{name: "IPv4 host without TLS host", mutate: func(c *CloudSQLIAMTLSConfig) { c.TlsHost = "" }},
		{name: "IPv6 host without TLS host", mutate: func(c *CloudSQLIAMTLSConfig) { c.Host, c.TlsHost = "[2001:db8::1]", "" }},
		{name: "empty host without TLS host", mutate: func(c *CloudSQLIAMTLSConfig) { c.Host, c.TlsHost = "", "" }},
		{name: "blank TLS host", mutate: func(c *CloudSQLIAMTLSConfig) { c.TlsHost = " " }},
		{name: "no root CA", mutate: func(c *CloudSQLIAMTLSConfig) { c.RootCa = " " }},
		{name: "TLS disabled", mutate: func(c *CloudSQLIAMTLSConfig) { c.DisableTls = true }},
		{name: "skip cert verification", mutate: func(c *CloudSQLIAMTLSConfig) { c.SkipCertVerification = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			if test.valid {
				require.NoError(t, config.Verify())
			} else {
				require.Error(t, config.Verify())
			}
		})
	}
}
