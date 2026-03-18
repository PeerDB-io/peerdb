package connmysql

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
)

const (
	toxiproxyDownProxyPort         = 42001
	toxiproxyLatencyProxyPort      = 42002
	toxiproxyResetProxyPort        = 42003
	toxiproxyCDCHangProxyPort      = 42004
	toxiproxyCDCCloseHangProxyPort = 42005
)

func setupMySQLConnectorWithProxy(ctx context.Context, t *testing.T, proxyName string, proxyPort int,
) (*MySqlConnector, *toxiproxy.Proxy) {
	t.Helper()

	toxiproxyClient := utils.NewToxiproxyClient(t)
	sshProxy := utils.CreateSSHProxy(t, toxiproxyClient, proxyName, proxyPort)

	connector, err := NewMySqlConnector(ctx, &protos.MySqlConfig{
		Host:     "mysql",
		Port:     3306,
		User:     "root",
		Password: "cipass",
		Database: "mysql",
		SshConfig: &protos.SSHConfig{
			Host:     "localhost",
			Port:     uint32(proxyPort),
			User:     "testuser",
			Password: "testpass",
		},
		DisableTls: true,
	})
	require.NoError(t, err)

	// Test initial connection works
	err = connector.ConnectionActive(ctx)
	require.NoError(t, err, "Initial connection should work")

	return connector, sshProxy
}

func setupMySQLConnectorWithSSH(ctx context.Context, t *testing.T, proxyName string, proxyPort int,
) (*MySqlConnector, utils.SSHKeepaliveTestConfig) {
	t.Helper()

	connector, sshProxy := setupMySQLConnectorWithProxy(ctx, t, proxyName, proxyPort)
	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)

	return connector, utils.SSHKeepaliveTestConfig{
		SSHProxy:      sshProxy,
		KeepaliveChan: keepaliveChan,
		RunLongQuery: func(ctx context.Context) error {
			_, err := connector.Execute(ctx, "SELECT SLEEP(60)")
			return err
		},
	}
}

func TestMySQLSSHKeepaliveWithToxiproxy(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	connector, cfg := setupMySQLConnectorWithSSH(t.Context(), t, "my-ssh-keepalive-test", toxiproxyDownProxyPort)
	defer connector.Close()
	utils.RunSSHKeepaliveDownTest(t, cfg)
}

func TestMySQLSSHKeepaliveLatency(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	connector, cfg := setupMySQLConnectorWithSSH(t.Context(), t, "my-ssh-latency-test", toxiproxyLatencyProxyPort)
	defer connector.Close()
	utils.RunSSHKeepaliveLatencyTest(t, cfg)
}

func TestMySQLSSHResetPeer(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	connector, cfg := setupMySQLConnectorWithSSH(t.Context(), t, "my-ssh-reset-peer-test", toxiproxyResetProxyPort)
	defer connector.Close()
	utils.RunSSHResetPeerTest(t, cfg)
}

func setupCDCPullRecords(
	ctx context.Context, t *testing.T, connector *MySqlConnector,
) (*model.PullRecordsRequest[model.RecordItems], *otel_metrics.OtelManager) {
	t.Helper()

	gtidOn, err := connector.GetGtidModeOn(ctx)
	require.NoError(t, err)

	var offsetText string
	if gtidOn {
		gset, err := connector.GetMasterGTIDSet(ctx)
		require.NoError(t, err)
		offsetText = gset.String()
	} else {
		pos, err := connector.GetMasterPos(ctx)
		require.NoError(t, err)
		offsetText = posToOffsetText(pos)
	}

	otelManager, err := otel_metrics.NewOtelManager(ctx, "test", false)
	require.NoError(t, err)

	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName:            "test_ssh_cdc",
		RecordStream:           model.NewCDCStream[model.RecordItems](100),
		TableNameMapping:       map[string]model.NameAndExclude{},
		TableNameSchemaMapping: map[string]*protos.TableSchema{},
		LastOffset:             model.CdcCheckpoint{Text: offsetText},
		MaxBatchSize:           10000,
		IdleTimeout:            time.Minute,
		ConsumedOffset:         &atomic.Int64{},
	}

	return req, otelManager
}

func TestMySQLSSHKeepaliveCDCHang(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}
	ctx := t.Context()

	connector, sshProxy := setupMySQLConnectorWithProxy(ctx, t, "my-ssh-cdc-down-test", toxiproxyCDCHangProxyPort)
	defer connector.Close()

	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)
	require.NotNil(t, keepaliveChan, "SSH keepalive channel should exist")

	req, otelManager := setupCDCPullRecords(ctx, t, connector)

	pullDone := concurrency.NewLatch[error]()
	go func() {
		pullDone.Set(connector.PullRecords(ctx, shared.CatalogPool{}, otelManager, req))
	}()
	go func() {
		for range req.RecordStream.GetRecords() {
		}
	}()

	time.Sleep(2 * time.Second)

	t.Log("Adding latency toxic to simulate network black hole during CDC streaming")
	_, err := sshProxy.AddToxic("latency", "latency", "", 1.0, toxiproxy.Attributes{
		"latency": 120000,
	})
	require.NoError(t, err)

	// Wait for keepalive to detect the failure first — if PullRecords exits before
	// the keepalive fires, something else closed the connection (driver timeout, etc.)
	select {
	case <-keepaliveChan:
		t.Log("SSH keepalive detected failure")
	case <-pullDone.Chan():
		t.Fatal("PullRecords exited before SSH keepalive fired — connection closed by something other than keepalive")
	case <-time.After(3 * utils.SSHKeepaliveInterval):
		t.Fatal("SSH keepalive did not fire in time")
	}

	select {
	case <-pullDone.Chan():
		pullErr := pullDone.Wait()
		require.Error(t, pullErr, "PullRecords should fail after SSH keepalive detects black hole")
		t.Logf("PullRecords returned: %v", pullErr)
	case <-time.After(10 * time.Second):
		t.Fatal("PullRecords did not return after SSH keepalive closed the connection")
	}
}

func TestMySQLSSHKeepaliveCDCCloseHang(t *testing.T) {
	if os.Getenv("CI_MYSQL_VERSION") == "maria" {
		t.Skip("Skipping SSH keepalive test for MariaDB")
	}

	ctx := t.Context()

	connector, sshProxy := setupMySQLConnectorWithProxy(ctx, t, "my-ssh-cdc-latency-test", toxiproxyCDCCloseHangProxyPort)
	defer connector.Close()

	keepaliveChan := connector.ssh.GetKeepaliveChan(ctx)
	require.NotNil(t, keepaliveChan, "SSH keepalive channel should exist")

	req, otelManager := setupCDCPullRecords(ctx, t, connector)

	// Use a short-lived context so PullRecords starts shutting down while
	// the connection is blocked by latency
	cancelCtx, cancel := context.WithTimeout(ctx, 7*time.Second)
	defer cancel()

	pullDone := concurrency.NewLatch[error]()
	go func() {
		pullDone.Set(connector.PullRecords(cancelCtx, shared.CatalogPool{}, otelManager, req))
	}()
	go func() {
		for range req.RecordStream.GetRecords() {
		}
	}()

	// Wait for CDC streaming to establish before adding latency
	time.Sleep(2 * time.Second)

	// Add high latency so the connection becomes unresponsive without erroring
	t.Log("Adding latency toxic to block the connection during CDC streaming and cleanup")
	_, err := sshProxy.AddToxic("latency", "latency", "", 1.0, toxiproxy.Attributes{
		"latency": 120000,
	})
	require.NoError(t, err)

	t.Logf("Waiting for context cancel (~5s), then PullRecords should hang until keepalive fires (within %s)", 3*utils.SSHKeepaliveInterval)

	// Wait for keepalive to detect the failure — PullRecords should be hanging
	// until keepalive closes the underlying connection
	select {
	case <-keepaliveChan:
		t.Log("SSH keepalive detected failure")
	case <-time.After(3 * utils.SSHKeepaliveInterval):
		t.Fatal("SSH keepalive did not fire in time")
	}

	select {
	case <-pullDone.Chan():
		pullErr := pullDone.Wait()
		require.ErrorIs(t, pullErr, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("PullRecords did not return after SSH keepalive closed the connection")
	}
}
