package e2e

import (
	"fmt"
	"io"
	"os"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type FlowConnectionGenerationConfig struct {
	FlowJobName      string
	TableNameMapping map[string]string
	PostgresPort     int
	BigQueryConfig   *protos.BigqueryConfig
}

type QRepFlowConnectionGenerationConfig struct {
	FlowJobName                string
	SourceTableIdentifier      string
	DestinationTableIdentifier string
	PostgresPort               int
	BigQueryConfig             *protos.BigqueryConfig
}

// GeneratePostgresPeer generates a postgres peer config for testing.
func GeneratePostgresPeer(postgresPort int) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_postgres_peer"
	ret.Type = protos.DBType_POSTGRES

	ret.Config = &protos.Peer_PostgresConfig{
		PostgresConfig: &protos.PostgresConfig{
			Host:     "localhost",
			Port:     uint32(postgresPort),
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		},
	}

	return ret
}

// readFileToBytes reads a file to a byte array.
func readFileToBytes(path string) ([]byte, error) {
	var ret []byte

	f, err := os.Open(path)
	if err != nil {
		return ret, fmt.Errorf("failed to open file: %w", err)
	}

	defer f.Close()

	ret, err = io.ReadAll(f)
	if err != nil {
		return ret, fmt.Errorf("failed to read file: %w", err)
	}

	return ret, nil
}

// GenerateBQPeer generates a bigquery peer config for testing.
func GenerateBQPeer(bigQueryConfig *protos.BigqueryConfig) (*protos.Peer, error) {
	ret := &protos.Peer{}
	ret.Name = "test_bq_peer"
	ret.Type = protos.DBType_BIGQUERY

	ret.Config = &protos.Peer_BigqueryConfig{
		BigqueryConfig: bigQueryConfig,
	}

	return ret, nil
}

func (c *FlowConnectionGenerationConfig) GenerateFlowConnectionConfigs() (*protos.FlowConnectionConfigs, error) {
	ret := &protos.FlowConnectionConfigs{}

	ret.FlowJobName = c.FlowJobName
	ret.TableNameMapping = c.TableNameMapping

	ret.Source = GeneratePostgresPeer(c.PostgresPort)

	bqPeer, err := GenerateBQPeer(c.BigQueryConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bq peer: %w", err)
	}

	ret.Destination = bqPeer

	return ret, nil
}

// GenerateQRepConfig generates a qrep config for testing.
func (c *QRepFlowConnectionGenerationConfig) GenerateQRepConfig(
	query string, watermark string, syncMode protos.QRepSyncMode) (*protos.QRepConfig, error) {
	ret := &protos.QRepConfig{}

	ret.FlowJobName = c.FlowJobName
	ret.SourceTableIdentifier = c.SourceTableIdentifier
	ret.DestinationTableIdentifier = c.DestinationTableIdentifier

	postgresPeer := GeneratePostgresPeer(c.PostgresPort)
	bqPeer, err := GenerateBQPeer(c.BigQueryConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bq peer: %w", err)
	}

	ret.SourcePeer = postgresPeer
	ret.DestinationPeer = bqPeer

	ret.Query = query
	ret.WatermarkColumn = watermark

	ret.SyncMode = syncMode

	return ret, nil
}
