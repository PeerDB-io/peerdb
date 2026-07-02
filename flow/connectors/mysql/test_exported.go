package connmysql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
)

// ConfigureReplication makes the test server's binlog and GTID settings match the
// replication mechanism in config. For non-MariaDB servers it steps gtid_mode to the
// required state one allowed transition at a time.
//
// It lives in a non-_test file because the e2e suite (an external package) calls it; the
// remaining test helpers are package-internal and live in helpers_test.go.
func ConfigureReplication(t *testing.T, connector *MySqlConnector, config *protos.MySqlConfig) error {
	t.Helper()
	setupSQL := []string{
		"set global binlog_format=row",
		"set global binlog_row_image=full",
		"set global max_connections=500",
	}

	if cmp, err := connector.CompareServerVersion(t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata); err != nil {
		return err
	} else if cmp >= 0 {
		setupSQL = append(setupSQL, "set global binlog_row_metadata=full")
	}

	if config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		rs, err := connector.Execute(t.Context(), "select @@gtid_mode")
		if err != nil {
			return err
		}
		gtidMode, err := rs.GetString(0, 0)
		if err != nil {
			return err
		}
		switch config.ReplicationMechanism {
		case protos.MySqlReplicationMechanism_MYSQL_GTID:
			if strings.EqualFold(gtidMode, "off") {
				// The value of @@GLOBAL.GTID_MODE can only be changed one step at a time:
				// OFF <-> OFF_PERMISSIVE <-> ON_PERMISSIVE <-> ON
				setupSQL = append(setupSQL,
					"select get_lock('settings',-1)",
					"set global enforce_gtid_consistency=on",
					"set global gtid_mode=off_permissive",
					"set global gtid_mode=on_permissive",
					"set global gtid_mode=on",
					"do release_lock('settings')",
				)
			}
		case protos.MySqlReplicationMechanism_MYSQL_FILEPOS:
			if strings.EqualFold(gtidMode, "on") {
				// The value of @@GLOBAL.GTID_MODE can only be changed one step at a time:
				// ON <-> ON_PERMISSIVE <-> OFF_PERMISSIVE <-> OFF
				setupSQL = append(setupSQL,
					"select get_lock('settings',-1)",
					"set global enforce_gtid_consistency=off",
					"set global gtid_mode=on_permissive",
					"set global gtid_mode=off_permissive",
					"set global gtid_mode=off",
					"do release_lock('settings')",
				)
			}
		default:
			return fmt.Errorf("unexpected replication mechanism: %v", config.ReplicationMechanism)
		}
	}

	for _, sql := range setupSQL {
		if _, err := connector.Execute(t.Context(), sql); err != nil {
			return fmt.Errorf("error executing %s: %w", sql, err)
		}
	}

	return nil
}
