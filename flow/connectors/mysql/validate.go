package connmysql

import (
	"context"
	"errors"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func (c *MySqlConnector) ValidateCheck(ctx context.Context) error {
	if _, err := c.Execute(ctx, "select @@gtid_mode"); err != nil {
		var mErr *mysql.MyError
		// seems to be MariaDB
		if errors.As(err, &mErr) && mErr.Code == mysql.ER_UNKNOWN_SYSTEM_VARIABLE && c.config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
			return errors.New("server appears to be MariaDB but flavor is not set to MariaDB")
		} else {
			return err
		}
	}
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		return errors.New("flavor is set to MariaDB but the server appears to be MySQL")
	} else if c.config.Flavor == protos.MySqlFlavor_MYSQL_UNKNOWN {
		return errors.New("flavor is set to unknown")
	}
	return nil
}

func (c *MySqlConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	return nil
}
