package pgwire

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/xdg-go/scram"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

const scramIterations = 4096

// authenticateSCRAM performs SCRAM-SHA-256 authentication with the client.
// This matches the security level of nexus (Rust pgwire).
func authenticateSCRAM(conn net.Conn, writeTimeout time.Duration) error {
	password := internal.PeerDBPgwirePassword()

	// Send AuthenticationSASL requesting SCRAM-SHA-256
	authSASL := &pgproto3.AuthenticationSASL{
		AuthMechanisms: []string{"SCRAM-SHA-256"},
	}
	if err := writeBackendMessage(conn, authSASL, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASL: %w", err)
	}

	// Receive SASLInitialResponse containing client-first-message.
	// SetAuthType tells pgproto3 to decode the 'p' message as SASLInitialResponse.
	backend := pgproto3.NewBackend(conn, conn)
	if err := backend.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return fmt.Errorf("failed to set auth type: %w", err)
	}
	msg, err := backend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL response: %w", err)
	}
	initialResp, ok := msg.(*pgproto3.SASLInitialResponse)
	if !ok {
		return fmt.Errorf("expected SASLInitialResponse, got %T", msg)
	}
	if initialResp.AuthMechanism != "SCRAM-SHA-256" {
		return fmt.Errorf("unsupported auth mechanism: %s", initialResp.AuthMechanism)
	}

	// Generate random salt and derive stored credentials from password
	saltBytes := make([]byte, 16)
	if _, err := rand.Read(saltBytes); err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}
	salt := base64.StdEncoding.EncodeToString(saltBytes)

	client, err := scram.SHA256.NewClient("", password, "")
	if err != nil {
		return fmt.Errorf("failed to create SCRAM client: %w", err)
	}
	storedCreds := client.GetStoredCredentials(scram.KeyFactors{
		Salt:  salt,
		Iters: scramIterations,
	})

	// Create SCRAM server (credential lookup ignores username, uses fixed password)
	server, err := scram.SHA256.NewServer(func(_ string) (scram.StoredCredentials, error) {
		return storedCreds, nil
	})
	if err != nil {
		return fmt.Errorf("failed to create SCRAM server: %w", err)
	}
	conv := server.NewConversation()

	// Process client-first-message, send server-first-message
	serverFirst, err := conv.Step(string(initialResp.Data))
	if err != nil {
		return fmt.Errorf("SCRAM client-first failed: %w", err)
	}

	authContinue := &pgproto3.AuthenticationSASLContinue{
		Data: []byte(serverFirst),
	}
	if err := writeBackendMessage(conn, authContinue, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASLContinue: %w", err)
	}

	// Receive SASLResponse containing client-final-message
	if err := backend.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return fmt.Errorf("failed to set auth type: %w", err)
	}
	msg, err = backend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL response: %w", err)
	}
	saslResp, ok := msg.(*pgproto3.SASLResponse)
	if !ok {
		return fmt.Errorf("expected SASLResponse, got %T", msg)
	}

	// Process client-final-message, verify authentication
	serverFinal, err := conv.Step(string(saslResp.Data))
	if err != nil {
		return fmt.Errorf("SCRAM client-final failed: %w", err)
	}

	if !conv.Valid() {
		return errors.New("authentication failed")
	}

	// Send server-final-message
	authFinal := &pgproto3.AuthenticationSASLFinal{
		Data: []byte(serverFinal),
	}
	if err := writeBackendMessage(conn, authFinal, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASLFinal: %w", err)
	}

	return nil
}
