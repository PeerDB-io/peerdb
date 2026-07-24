package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func parseTemporalCertAndKeyFromEnvironment(ctx context.Context) ([]tls.Certificate, error) {
	certBytes, err := internal.PeerDBTemporalClientCert(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get temporal certificate: %w", err)
	}

	keyBytes, err := internal.PeerDBTemporalClientKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get temporal key: %w", err)
	}

	keyPair, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain temporal key pair: %w", err)
	}

	return []tls.Certificate{keyPair}, nil
}

func setupTemporalClient(ctx context.Context, clientOptions client.Options) (client.Client, error) {
	var tlsConfig *tls.Config

	if certPath := internal.PeerDBTemporalClientCertPath(); certPath != "" {
		slog.InfoContext(ctx, "Using temporal certificate/key from paths for authentication")
		keyPath := internal.PeerDBTemporalClientKeyPath()

		tlsConfig = &tls.Config{
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				keyPairValue, err := tls.LoadX509KeyPair(certPath, keyPath)
				if err != nil {
					return nil, fmt.Errorf("unable to obtain temporal key pair: %w", err)
				}
				return &keyPairValue, nil
			},
			MinVersion: tls.VersionTLS13,
		}
	} else if internal.PeerDBTemporalEnableCertAuth() {
		slog.InfoContext(ctx, "Using temporal certificate/key from environment for authentication")

		certs, err := parseTemporalCertAndKeyFromEnvironment(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to base64 decode certificate and key: %w", err)
		}

		tlsConfig = &tls.Config{
			Certificates: certs,
			MinVersion:   tls.VersionTLS13,
		}
	}

	if tlsConfig != nil {
		if serverName := internal.PeerDBTemporalTLSServerName(); serverName != "" {
			tlsConfig.ServerName = serverName
		}
		clientOptions.ConnectionOptions = client.ConnectionOptions{TLS: tlsConfig}
	}

	tc, err := client.Dial(clientOptions)
	return tc, err
}
