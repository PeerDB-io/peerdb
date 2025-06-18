package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

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
	if certPath := internal.PeerDBTemporalClientCertPath(); certPath != "" {
		slog.Info("Using temporal certificate/key from paths for authentication")
		keyPath := internal.PeerDBTemporalClientKeyPath()
		var certMtime, keyMtime time.Time
		var keyPair *tls.Certificate

		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: &tls.Config{
				GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
					if keyPair != nil {
						if certInfo, err := os.Stat(certPath); err == nil && certInfo.ModTime().Equal(certMtime) {
							if keyInfo, err := os.Stat(keyPath); err == nil && keyInfo.ModTime().Equal(keyMtime) {
								return keyPair, nil
							}
						}
					}

					certFile, err := os.Open(certPath)
					defer func() {
						if err := certFile.Close(); err != nil {
							slog.Warn("could not close temporal client cert", slog.Any("error", err))
						}
					}()
					if err != nil {
						return nil, fmt.Errorf("could not open temporal client cert %s: %w", certPath, err)
					}
					certBytes, err := io.ReadAll(certFile)
					if err != nil {
						return nil, fmt.Errorf("could not read temporal client cert %s: %w", certPath, err)
					}

					keyFile, err := os.Open(keyPath)
					defer func() {
						if err := keyFile.Close(); err != nil {
							slog.Warn("could not close temporal client key", slog.Any("error", err))
						}
					}()
					if err != nil {
						return nil, fmt.Errorf("could not open temporal client key %s: %w", keyPath, err)
					}
					keyBytes, err := io.ReadAll(keyFile)
					if err != nil {
						return nil, fmt.Errorf("could not read temporal client key %s: %w", keyPath, err)
					}

					keyPairValue, err := tls.X509KeyPair(certBytes, keyBytes)
					if err != nil {
						return nil, fmt.Errorf("unable to obtain temporal key pair: %w", err)
					}
					certStat, err := certFile.Stat()
					if err != nil {
						slog.Warn("unable to stat temporal cert, not caching", slog.Any("error", err))
						return &keyPairValue, nil
					}
					keyStat, err := keyFile.Stat()
					if err != nil {
						slog.Warn("unable to stat temporal key, not caching", slog.Any("error", err))
						return &keyPairValue, nil
					}
					keyPair = &keyPairValue
					certMtime = certStat.ModTime()
					keyMtime = keyStat.ModTime()
					return keyPair, nil
				},
				MinVersion: tls.VersionTLS13,
			},
		}
	} else if internal.PeerDBTemporalEnableCertAuth() {
		slog.Info("Using temporal certificate/key from environment for authentication")

		certs, err := parseTemporalCertAndKeyFromEnvironment(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to base64 decode certificate and key: %w", err)
		}

		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: &tls.Config{
				Certificates: certs,
				MinVersion:   tls.VersionTLS13,
			},
		}
	}

	tc, err := client.Dial(clientOptions)
	return tc, err
}
