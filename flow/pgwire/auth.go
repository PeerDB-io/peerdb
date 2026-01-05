package pgwire

import (
	"crypto/rand"
	"encoding/base64"
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

	// 1. Send AuthenticationSASL with SCRAM-SHA-256
	authSASL := &pgproto3.AuthenticationSASL{
		AuthMechanisms: []string{"SCRAM-SHA-256"},
	}
	if err := writeBackendMessage(conn, authSASL, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASL: %w", err)
	}

	// 2. Receive SASLInitialResponse (client-first-message)
	backend := pgproto3.NewBackend(conn, conn)
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

	// 3. Create SCRAM server conversation
	// Generate random salt (16 bytes, base64 encoded)
	saltBytes := make([]byte, 16)
	if _, err := rand.Read(saltBytes); err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}
	salt := base64.StdEncoding.EncodeToString(saltBytes)

	// Create stored credentials from password
	client, err := scram.SHA256.NewClient("", password, "")
	if err != nil {
		return fmt.Errorf("failed to create SCRAM client: %w", err)
	}
	storedCreds := client.GetStoredCredentials(scram.KeyFactors{
		Salt:  salt,
		Iters: scramIterations,
	})

	// Create server with credential lookup (ignores username, uses fixed password)
	server, err := scram.SHA256.NewServer(func(_ string) (scram.StoredCredentials, error) {
		return storedCreds, nil
	})
	if err != nil {
		return fmt.Errorf("failed to create SCRAM server: %w", err)
	}
	conv := server.NewConversation()

	// 4. Process client-first, send server-first
	serverFirst, err := conv.Step(string(initialResp.Data))
	if err != nil {
		return fmt.Errorf("SCRAM step 1 failed: %w", err)
	}

	authContinue := &pgproto3.AuthenticationSASLContinue{
		Data: []byte(serverFirst),
	}
	if err := writeBackendMessage(conn, authContinue, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASLContinue: %w", err)
	}

	// 5. Receive client-final
	msg, err = backend.Receive()
	if err != nil {
		return fmt.Errorf("failed to receive SASL response: %w", err)
	}
	saslResp, ok := msg.(*pgproto3.SASLResponse)
	if !ok {
		return fmt.Errorf("expected SASLResponse, got %T", msg)
	}

	// 6. Process client-final, send server-final
	serverFinal, err := conv.Step(string(saslResp.Data))
	if err != nil {
		return fmt.Errorf("SCRAM step 2 failed: %w", err)
	}

	if !conv.Valid() {
		return fmt.Errorf("authentication failed")
	}

	authFinal := &pgproto3.AuthenticationSASLFinal{
		Data: []byte(serverFinal),
	}
	if err := writeBackendMessage(conn, authFinal, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASLFinal: %w", err)
	}

	return nil
}
