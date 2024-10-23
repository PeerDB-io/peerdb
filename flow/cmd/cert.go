package cmd

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

func parseTemporalCertAndKey(ctx context.Context) ([]tls.Certificate, error) {
	certBytes, err := peerdbenv.PeerDBTemporalClientCert(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get temporal certificate: %w", err)
	}

	keyBytes, err := peerdbenv.PeerDBTemporalClientKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get temporal key: %w", err)
	}

	keyPair, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain temporal key pair: %w", err)
	}

	return []tls.Certificate{keyPair}, nil
}
