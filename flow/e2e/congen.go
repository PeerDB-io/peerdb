package e2e

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

// GeneratePostgresPeer generates a postgres peer config for testing.
func GeneratePostgresPeer(postgresPort int) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_postgres_peer"
	ret.Type = protos.DBType_POSTGRES

	ret.Config = &protos.Peer_PostgresConfig{
		PostgresConfig: &protos.PostgresConfig{
			Host:     "192.168.29.185",
			Port:     uint32(postgresPort),
			User:     "postgres",
			Password: "banana",
			Database: "peerdb",
		},
	}

	return ret
}

type FlowConnectionGenerationConfig struct {
	FlowJobName      string
	TableNameMapping map[string]string
	PostgresPort     int
	Destination      *protos.Peer
}

// GenerateSnowflakePeer generates a snowflake peer config for testing.
func GenerateSnowflakePeer(snowflakeConfig *protos.SnowflakeConfig) (*protos.Peer, error) {
	ret := &protos.Peer{}
	ret.Name = "test_snowflake_peer"
	ret.Type = protos.DBType_SNOWFLAKE

	ret.Config = &protos.Peer_SnowflakeConfig{
		SnowflakeConfig: snowflakeConfig,
	}

	return ret, nil
}

func (c *FlowConnectionGenerationConfig) GenerateFlowConnectionConfigs() (*protos.FlowConnectionConfigs, error) {
	ret := &protos.FlowConnectionConfigs{}
	ret.FlowJobName = c.FlowJobName
	ret.TableNameMapping = c.TableNameMapping
	ret.Source = GeneratePostgresPeer(c.PostgresPort)
	ret.Destination = c.Destination
	return ret, nil
}

type QRepFlowConnectionGenerationConfig struct {
	FlowJobName                string
	WatermarkTable             string
	DestinationTableIdentifier string
	PostgresPort               int
	Destination                *protos.Peer
}

// GenerateQRepConfig generates a qrep config for testing.
func (c *QRepFlowConnectionGenerationConfig) GenerateQRepConfig(
	query string, watermark string, syncMode protos.QRepSyncMode) (*protos.QRepConfig, error) {
	ret := &protos.QRepConfig{}
	ret.FlowJobName = c.FlowJobName
	ret.WatermarkTable = c.WatermarkTable
	ret.DestinationTableIdentifier = c.DestinationTableIdentifier

	postgresPeer := GeneratePostgresPeer(c.PostgresPort)
	ret.SourcePeer = postgresPeer

	ret.DestinationPeer = c.Destination

	ret.Query = query
	ret.WatermarkColumn = watermark

	ret.SyncMode = syncMode

	return ret, nil
}
