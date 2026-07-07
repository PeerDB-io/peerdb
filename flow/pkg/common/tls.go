package common

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
)

// modified from https://github.com/golang/go/blob/master/src/crypto/tls/example_test.go
// https://github.com/PeerDB-io/peerdb/pull/2805
func verifyPeerCertificateWithoutHostname(rootCAs *x509.CertPool) func(certificates [][]byte, _ [][]*x509.Certificate) error {
	return func(certificates [][]byte, _ [][]*x509.Certificate) error {
		opts := x509.VerifyOptions{
			Roots:         rootCAs,
			DNSName:       "",
			Intermediates: x509.NewCertPool(),
		}
		var cert0 *x509.Certificate
		for i, asn1Data := range certificates {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return fmt.Errorf("tls: failed to parse certificate from server: %w", err)
			}
			if i == 0 {
				cert0 = cert
			} else {
				opts.Intermediates.AddCert(cert)
			}
		}
		_, err := cert0.Verify(opts)
		return err
	}
}

// ClientCertificate is a PEM-encoded client certificate and its private key, for mutual TLS.
type ClientCertificate struct {
	certificate string
	privateKey  string
}

// NewClientCertificate requires both the certificate and private key to be non-empty.
func NewClientCertificate(certificate string, privateKey string) (*ClientCertificate, error) {
	if certificate == "" || privateKey == "" {
		return nil, errors.New("both certificate and private key must be provided for client certificate authentication")
	}
	return &ClientCertificate{certificate: certificate, privateKey: privateKey}, nil
}

func CreateTlsConfig(
	minVersion uint16, rootCAs *string, host string, tlsHost string, skipCertVerification bool,
	clientCert *ClientCertificate,
) (*tls.Config, error) {
	//nolint:gosec
	config := &tls.Config{MinVersion: minVersion}
	if rootCAs != nil {
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM([]byte(*rootCAs)) {
			return nil, fmt.Errorf("failed to parse provided root CA")
		}
		config.RootCAs = caPool
	}
	if clientCert != nil {
		cert, err := tls.X509KeyPair([]byte(clientCert.certificate), []byte(clientCert.privateKey))
		if err != nil {
			return nil, fmt.Errorf("failed to parse provided client certificate and private key: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}
	if skipCertVerification {
		// self-hosted instances may generate self-signed certs that can't be verified
		// but can still be used for TLS — this must be explicitly requested by the user
		config.InsecureSkipVerify = true
	} else if tlsHost != "" {
		config.ServerName = tlsHost
	} else if net.ParseIP(host) == nil {
		// set to server host when it is a hostname (and not an IP address)
		config.ServerName = host
	} else {
		// host is a raw IP address (e.g. GCP Cloud SQL)
		// so we verify the certificate chain ourselves without checking the hostname
		config.InsecureSkipVerify = true
		config.VerifyPeerCertificate = verifyPeerCertificateWithoutHostname(config.RootCAs)
	}
	return config, nil
}

// CreateTlsConfigFromRootCAString adapts CreateTlsConfig for callers that pass rootCAs as a string
// rather than *string (e.g. mongo ClientConfig). Empty string is treated as no custom CA.
func CreateTlsConfigFromRootCAString(minVersion uint16, rootCAs string, host string, tlsHost string, skipCertVerification bool) (*tls.Config, error) {
	var rootCAsPtr *string
	if rootCAs != "" {
		rootCAsPtr = &rootCAs
	}
	return CreateTlsConfig(minVersion, rootCAsPtr, host, tlsHost, skipCertVerification, nil)
}
