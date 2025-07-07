package mysql

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func CompareServerVersion(conn *client.Conn, version string) (int, error) {
	rr, err := conn.Execute("SELECT version()")
	if err != nil {
		return 0, fmt.Errorf("failed to get server version: %w", err)
	}

	serverVersion, err := rr.GetString(0, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to get server version: %w", err)
	}

	cmp, err := mysql.CompareServerVersions(serverVersion, version)
	if err != nil {
		return 0, fmt.Errorf("failed to compare server version: %w", err)
	}
	return cmp, nil
}

func ValidateFlavor(conn *client.Conn, flavor protos.MySqlFlavor) error {
	// MariaDB specific setting, introduced in MariaDB 10.0.3
	if rs, err := conn.Execute("SELECT @@gtid_strict_mode"); err != nil {
		var mErr *mysql.MyError
		// seems to be MySQL
		if errors.As(err, &mErr) && mErr.Code == mysql.ER_UNKNOWN_SYSTEM_VARIABLE {
			if flavor != protos.MySqlFlavor_MYSQL_MYSQL {
				return errors.New("server appears to be MySQL but MariaDB source has been selected")
			}
		} else {
			return fmt.Errorf("failed to check GTID mode: %w", err)
		}
	} else if len(rs.Values) > 0 {
		if flavor != protos.MySqlFlavor_MYSQL_MARIA {
			return errors.New("server appears to be MariaDB but MySQL source has been selected")
		}
	}

	return nil
}

// only for MySQL 5.7 and below
func CheckMySQL5BinlogSettings(conn *client.Conn, logger log.Logger) error {
	if cmp, err := CompareServerVersion(conn, "5.5.0"); err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	} else if cmp < 0 {
		// TODO: revisit this when we have more data
		logger.Warn("cannot validate mysql prior to 5.5.0, bypass")
		return nil
	}

	// MySQL 5.6.6 introduced GTIDs so they may work, no need to enforce filepos for now

	query := "SELECT @@binlog_format"
	checkBinlogRowImage := false

	if cmp, err := CompareServerVersion(conn, "5.6.2"); err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	} else if cmp >= 0 {
		query += ", @@binlog_row_image"
		checkBinlogRowImage = true
	}

	// binlog_expire_logs_seconds was introduced in 8.0 https://dev.mysql.com/worklog/task/?id=10924
	// since expire_logs_days has day granularity, all settings of it work for us so not checking
	rs, err := conn.Execute(query)
	if err != nil {
		return fmt.Errorf("failed to retrieve settings <5.7: %w", err)
	}
	if len(rs.Values) == 0 {
		return errors.New("no value returned for settings <5.7")
	}
	row := rs.Values[0]

	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(row[0].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW', currently " + binlogFormat)
	}
	if checkBinlogRowImage {
		binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(row[1].AsString())
		if binlogRowImage != "FULL" {
			return errors.New("binlog_row_image must be set to 'FULL', currently " + binlogRowImage)
		}
	}

	return nil
}

func CheckMySQL8BinlogSettings(conn *client.Conn, logger log.Logger) error {
	cmp, err := CompareServerVersion(conn, "8.0.1")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp < 0 {
		logger.Warn("MySQL less than 8.0.1, checking binlog settings <5.7")
		return CheckMySQL5BinlogSettings(conn, logger)
	}

	query := "SELECT @@binlog_expire_logs_seconds, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata"
	// check if binlog_row_value_options is available
	checkRowValueOptions := false
	if cmp, err := CompareServerVersion(conn, "8.0.3"); err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	} else if cmp >= 0 {
		checkRowValueOptions = true
		query += ", @@binlog_row_value_options"
	}

	rs, err := conn.Execute(query)
	if err != nil {
		return fmt.Errorf("failed to retrieve settings: %w", err)
	}
	if len(rs.Values) == 0 {
		return errors.New("no value returned for settings")
	}
	row := rs.Values[0]

	binlogExpireLogsSeconds := row[0].AsUint64()
	if binlogExpireLogsSeconds < 86400 && binlogExpireLogsSeconds != 0 {
		return errors.New(
			"binlog_expire_logs_seconds must be set to at least 86400 (24 hours), currently " +
				strconv.FormatUint(binlogExpireLogsSeconds, 10))
	}
	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(row[1].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW', currently " + binlogFormat)
	}
	binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(row[2].AsString())
	if binlogRowImage != "FULL" {
		return errors.New("binlog_row_image must be set to 'FULL', currently " + binlogRowImage)
	}
	binlogRowMetadata := shared.UnsafeFastReadOnlyBytesToString(row[3].AsString())
	if binlogRowMetadata != "FULL" {
		return errors.New("binlog_row_metadata must be set to 'FULL', currently " + binlogRowMetadata)
	}
	if checkRowValueOptions {
		binlogRowValueOptions := shared.UnsafeFastReadOnlyBytesToString(row[4].AsString())
		if binlogRowValueOptions != "" {
			return errors.New("binlog_row_value_options must be disabled, currently " + binlogRowValueOptions)
		}
	}

	return nil
}

func CheckMariaDBBinlogSettings(conn *client.Conn, logger log.Logger) error {
	query := "SELECT @@binlog_format, @@binlog_row_image, @@binlog_row_metadata"

	checkBinlogExpiry := false
	cmp, err := CompareServerVersion(conn, "10.6.1")
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	if cmp >= 0 {
		checkBinlogExpiry = true
		query += ", @@binlog_expire_logs_seconds"
	} else {
		logger.Warn("MariaDB version does not support binlog_expire_logs_seconds, skipping check")
	}

	rs, err := conn.Execute(query)
	if err != nil {
		return fmt.Errorf("failed to retrieve settings: %w", err)
	}
	if len(rs.Values) == 0 {
		return errors.New("no value returned for settings")
	}
	row := rs.Values[0]

	binlogFormat := shared.UnsafeFastReadOnlyBytesToString(row[0].AsString())
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be set to 'ROW', currently " + binlogFormat)
	}

	binlogRowImage := shared.UnsafeFastReadOnlyBytesToString(row[1].AsString())
	if binlogRowImage != "FULL" {
		return errors.New("binlog_row_image must be set to 'FULL', currently " + binlogRowImage)
	}

	binlogRowMetadata := shared.UnsafeFastReadOnlyBytesToString(row[2].AsString())
	if binlogRowMetadata != "FULL" {
		// only strictly required for column exclusion support, but let's enforce it for consistency
		return errors.New("binlog_row_metadata must be set to 'FULL', currently " + binlogRowMetadata)
	}

	if checkBinlogExpiry {
		binlogExpireLogsSeconds := row[3].AsUint64()
		if binlogExpireLogsSeconds < 86400 && binlogExpireLogsSeconds != 0 {
			return errors.New(
				"binlog_expire_logs_seconds must be set to at least 86400 (24 hours), currently " +
					strconv.FormatUint(binlogExpireLogsSeconds, 10))
		}
	}

	return nil
}

func CheckRDSBinlogSettings(conn *client.Conn, logger log.Logger) error {
	// AWS RDS/Aurora has its own binlog retention setting that we need to check, minimum 24h
	// check RDS/Aurora binlog retention setting
	if rs, err := conn.Execute("SELECT value FROM mysql.rds_configuration WHERE name='binlog retention hours'"); err != nil {
		var mErr *mysql.MyError
		if errors.As(err, &mErr) && (mErr.Code == mysql.ER_NO_SUCH_TABLE || mErr.Code == mysql.ER_TABLEACCESS_DENIED_ERROR) {
			// Table doesn't exist, which means this is not RDS/Aurora
			logger.Warn("mysql.rds_configuration table does not exist, skipping Aurora/RDS binlog retention check",
				slog.Any("error", err))
			return nil
		}
		return errors.New("failed to check RDS/Aurora binlog retention hours: " + err.Error())
	} else if len(rs.Values) > 0 {
		binlogRetentionHoursStr := shared.UnsafeFastReadOnlyBytesToString(rs.Values[0][0].AsString())
		if binlogRetentionHoursStr == "" {
			return errors.New("RDS/Aurora setting 'binlog retention hours' should be at least 24, currently unset")
		}
		if binlogRetentionHours, err := strconv.Atoi(binlogRetentionHoursStr); err != nil {
			return errors.New("failed to parse RDS/Aurora setting 'binlog retention hours': " + err.Error())
		} else if binlogRetentionHours < 24 {
			return errors.New("RDS/Aurora setting 'binlog retention hours' should be at least 24, currently " + binlogRetentionHoursStr)
		}
	} else {
		logger.Warn("binlog retention hours returned nothing, skipping Aurora/RDS binlog retention check")
	}

	return nil
}
