package cmd

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func parseTemporalCertAndKey(ctx context.Context) ([]tls.Certificate, error) {
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
