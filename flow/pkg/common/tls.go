package common

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
)

// Modified from https://github.com/golang/go/blob/master/src/crypto/tls/example_test.go.
// https://github.com/PeerDB-io/peerdb/pull/2805
func verifyConnectionWithoutHostname(rootCAs *x509.CertPool) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("tls: server provided no certificates")
		}

		opts := x509.VerifyOptions{
			Roots:         rootCAs,
			DNSName:       "",
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range state.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := state.PeerCertificates[0].Verify(opts)
		return err
	}
}

// ClientCertificate is a PEM-encoded client certificate and its private key, for mutual TLS.
type ClientCertificate struct {
	certificate string
	privateKey  string
}

type tlsConfigOptions struct {
	verifyCertificateChainOnly bool
}

// TLSConfigOption customizes TLS verification without changing the default behavior.
type TLSConfigOption func(*tlsConfigOptions)

// WithCertificateChainOnlyVerification verifies the server chain against the provided root CA without hostname matching.
// The caller-provided per-instance CA is the server identity anchor; broad or shared roots are not safe for this mode.
func WithCertificateChainOnlyVerification() TLSConfigOption {
	return func(options *tlsConfigOptions) {
		options.verifyCertificateChainOnly = true
	}
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
	clientCert *ClientCertificate, options ...TLSConfigOption,
) (*tls.Config, error) {
	configOptions := tlsConfigOptions{}
	for _, option := range options {
		option(&configOptions)
	}
	if configOptions.verifyCertificateChainOnly {
		if skipCertVerification {
			return nil, errors.New("certificate chain verification cannot be combined with skip certificate verification")
		}
		if rootCAs == nil || strings.TrimSpace(*rootCAs) == "" {
			return nil, errors.New("certificate chain verification requires a non-empty root CA")
		}
	}

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
	if configOptions.verifyCertificateChainOnly {
		config.InsecureSkipVerify = true
		config.VerifyConnection = verifyConnectionWithoutHostname(config.RootCAs)
	} else if skipCertVerification {
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
		config.VerifyConnection = verifyConnectionWithoutHostname(config.RootCAs)
	}
	return config, nil
}

// CreateTlsConfigFromRootCAString adapts CreateTlsConfig for callers that pass rootCAs as a string
// rather than *string (e.g. mongo ClientConfig). Empty string is treated as no custom CA.
func CreateTlsConfigFromRootCAString(
	minVersion uint16,
	rootCAs string,
	host string,
	tlsHost string,
	skipCertVerification bool,
) (*tls.Config, error) {
	var rootCAsPtr *string
	if rootCAs != "" {
		rootCAsPtr = &rootCAs
	}
	return CreateTlsConfig(minVersion, rootCAsPtr, host, tlsHost, skipCertVerification, nil)
}
