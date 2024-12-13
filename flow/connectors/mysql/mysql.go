// https://airbyte.com/blog/replicating-mysql-a-look-at-the-binlog-and-gtids

package connmysql

import (
	"context"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type MySqlConnector struct {
	config   *protos.MySqlConfig
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
}

func NewMySqlConnector(ctx context.Context, config *protos.MySqlConfig) (*MySqlConnector, error) {
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 1729,    // TODO put in config
		Flavor:   "mysql", // TODO put in config
		Host:     config.Host,
		Port:     uint16(config.Port),
		User:     config.User,
		Password: config.Password,
	})
	return &MySqlConnector{
		config: config,
		syncer: syncer,
	}, nil
}

func (c *MySqlConnector) Close() error {
	if c.syncer != nil {
		c.syncer.Close()
	}
	return nil
}

func (*MySqlConnector) ConnectionActive(context.Context) error {
	return nil
}
