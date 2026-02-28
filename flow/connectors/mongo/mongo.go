package connmongo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	peerdb_mongo "github.com/PeerDB-io/peerdb/flow/pkg/mongo"
)

const (
	DefaultDocumentKeyColumnName  = "_id"
	DefaultFullDocumentColumnName = "doc"
	LegacyFullDocumentColumnName  = "_full_document"
)

var protoReadPrefToString = map[protos.ReadPreference]string{
	protos.ReadPreference_PRIMARY:             peerdb_mongo.ReadPreferencePrimary,
	protos.ReadPreference_PRIMARY_PREFERRED:   peerdb_mongo.ReadPreferencePrimaryPreferred,
	protos.ReadPreference_SECONDARY:           peerdb_mongo.ReadPreferenceSecondary,
	protos.ReadPreference_SECONDARY_PREFERRED: peerdb_mongo.ReadPreferenceSecondaryPreferred,
	protos.ReadPreference_NEAREST:             peerdb_mongo.ReadPreferenceNearest,
	protos.ReadPreference_PREFERENCE_UNKNOWN:  peerdb_mongo.ReadPreferenceSecondaryPreferred,
}

var protoToReadPref = map[protos.ReadPreference]*readpref.ReadPref{
	protos.ReadPreference_PRIMARY:             readpref.Primary(),
	protos.ReadPreference_PRIMARY_PREFERRED:   readpref.PrimaryPreferred(),
	protos.ReadPreference_SECONDARY:           readpref.Secondary(),
	protos.ReadPreference_SECONDARY_PREFERRED: readpref.SecondaryPreferred(),
	protos.ReadPreference_NEAREST:             readpref.Nearest(),
	protos.ReadPreference_PREFERENCE_UNKNOWN:  readpref.SecondaryPreferred(),
}

type MongoConnector struct {
	logger log.Logger
	*metadataStore.PostgresMetadata
	config         *protos.MongoConfig
	client         *mongo.Client
	ssh            *utils.SSHTunnel
	totalBytesRead atomic.Int64
	deltaBytesRead atomic.Int64
}

func NewMongoConnector(ctx context.Context, config *protos.MongoConfig) (*MongoConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	mc := &MongoConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		logger:           logger,
	}

	sshTunnel, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}
	mc.ssh = sshTunnel

	var meteredDialer utils.MeteredDialer
	if sshTunnel != nil && sshTunnel.Client != nil {
		meteredDialer = utils.NewMeteredDialer(&mc.totalBytesRead, &mc.deltaBytesRead, sshTunnel.Client.DialContext, true)
	} else {
		meteredDialer = utils.NewMeteredDialer(&mc.totalBytesRead, &mc.deltaBytesRead, (&net.Dialer{Timeout: time.Minute}).DialContext, false)
	}

	clientOptions, err := peerdb_mongo.BuildClientOptions(peerdb_mongo.ClientConfig{
		Uri:                 config.Uri,
		Username:            config.Username,
		Password:            config.Password,
		ReadPreference:      protoReadPrefToString[config.ReadPreference],
		DisableTls:          config.DisableTls,
		RootCa:              config.GetRootCa(),
		TlsHost:             config.TlsHost,
		CreateTlsConfigFunc: common.CreateTlsConfigFromRootCAString,
		Dialer:              &meteredDialer,
	})
	if err != nil {
		return nil, err
	}

	if level, ok := os.LookupEnv("PEERDB_LOG_LEVEL"); ok && level == "DEBUG" {
		clientOptions.SetMonitor(NewCommandMonitor(logger))
	}

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	mc.client = client

	return mc, nil
}

func (c *MongoConnector) Close() error {
	var errs []error
	if c != nil && c.client != nil {
		// Use a timeout to ensure the disconnect operation does not hang indefinitely
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.client.Disconnect(timeout); err != nil {
			c.logger.Error("failed to disconnect MongoDB client", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to disconnect MongoDB client: %w", err))
		}
	}

	if err := c.ssh.Close(); err != nil {
		c.logger.Error("[mongo] failed to close SSH tunnel", slog.Any("error", err))
		errs = append(errs, fmt.Errorf("[mongo] failed to close SSH tunnel: %w", err))
	}
	return errors.Join(errs...)
}

func (c *MongoConnector) ConnectionActive(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	return nil
}

func (c *MongoConnector) GetVersion(ctx context.Context) (string, error) {
	buildInfo, err := peerdb_mongo.GetBuildInfo(ctx, c.client)
	if err != nil {
		return "", err
	}
	return buildInfo.Version, nil
}

func (c *MongoConnector) GetLogRetentionHours(ctx context.Context) (float64, error) {
	serverStatus, err := peerdb_mongo.GetServerStatus(ctx, c.client)
	if err != nil {
		return 0, fmt.Errorf("failed to get server status: %w", err)
	}

	return float64(serverStatus.OplogTruncation.OplogMinRetentionHours), nil
}

func (c *MongoConnector) GetTableSizeEstimatedBytes(ctx context.Context, tableIdentifier string) (int64, error) {
	parsedTable, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return 0, err
	}
	collStats, err := peerdb_mongo.GetCollStats(ctx, c.client, parsedTable.Namespace, parsedTable.Table)
	if err != nil {
		return 0, err
	}
	return collStats.Size, nil
}

// GetServerSideCommitLagMicroseconds returns the commit lag between the latest WAL write
// and the last consumed event. Both timestamps come from the MongoDB server to avoid clock skew.
func (c *MongoConnector) GetServerSideCommitLagMicroseconds(ctx context.Context, flowJobName string) (int64, error) {
	replSetStatus, err := peerdb_mongo.GetReplSetStatus(ctx, c.client)
	if err != nil {
		return 0, fmt.Errorf("failed to get replica set status: %w", err)
	}

	lastOffset, err := c.GetLastOffset(ctx, flowJobName)
	if err != nil {
		return 0, fmt.Errorf("failed to get last offset: %w", err)
	}

	if lastOffset.Text == "" {
		return 0, errors.New("last offset is empty string, cannot calculate commit lag")
	}

	resumeToken, err := base64.StdEncoding.DecodeString(lastOffset.Text)
	if err != nil {
		return 0, fmt.Errorf("failed to decode resume token: %w", err)
	}
	clusterTime, err := decodeTimestampFromResumeToken(resumeToken)
	if err != nil {
		return 0, fmt.Errorf("failed to decode timestamp from resume token: %w", err)
	}

	latestWALTime := replSetStatus.OpTimes.LastCommittedOpTime.Ts

	lagSeconds := max(int64(latestWALTime.T)-int64(clusterTime.T), 0)

	lagMicroseconds := lagSeconds * 1_000_000

	return lagMicroseconds, nil
}
