package connmysql

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestMapQValue(t *testing.T) {
	flavorToVersion := map[protos.MySqlFlavor]map[string]string{
		protos.MySqlFlavor_MYSQL_MYSQL: {
			"with_metadata":    mysql_validation.MySQLMinVersionForBinlogRowMetadata,
			"without_metadata": "5.7.42",
		},
		protos.MySqlFlavor_MYSQL_MARIA: {
			"with_metadata":    mysql_validation.MariaDBMinVersionForBinlogRowMetadata,
			"without_metadata": "9.0",
		},
	}

	testCases := []struct {
		name                string
		qv                  types.QValue
		expected            types.QValue
		field               types.QField
		err                 bool
		versionWithMetadata bool
	}{
		{
			name: "without metadata, should convert enum value to it's index",
			qv: types.QValueEnum{
				Val: "small",
			},
			field: types.QField{
				Name: "size",
			},
			expected: types.QValueEnum{
				Val: "1",
			},
			versionWithMetadata: false,
		},
		{
			name: "invalid enum value",
			qv: types.QValueEnum{
				Val: "xlarge",
			},
			field: types.QField{
				Name: "size",
			},
			err:                 true,
			versionWithMetadata: false,
		},
		{
			name: "should skip non-enum value",
			qv: types.QValueString{
				Val: "small",
			},
			field: types.QField{
				Name: "size",
			},
			expected: types.QValueString{
				Val: "small",
			},
			versionWithMetadata: false,
		},
		{
			name: "test that enum value stays the same on versions with binlog support",
			qv: types.QValueEnum{
				Val: "small",
			},
			field: types.QField{
				Name: "size",
			},
			expected: types.QValueEnum{
				Val: "small",
			},
			versionWithMetadata: true,
		},
		{
			name: "invalid enum value with metadata should pass through unchanged",
			qv: types.QValueEnum{
				Val: "xlarge",
			},
			field: types.QField{
				Name: "size",
			},
			expected: types.QValueEnum{
				Val: "xlarge",
			},
			versionWithMetadata: true,
		},
		{
			name: "enum column not in map should error",
			qv: types.QValueEnum{
				Val: "red",
			},
			field: types.QField{
				Name: "color",
			},
			err:                 true,
			versionWithMetadata: false,
		},
		{
			name: "empty enum value should pass through unchanged",
			qv: types.QValueEnum{
				Val: "",
			},
			field: types.QField{
				Name: "size",
			},
			expected: types.QValueEnum{
				Val: "",
			},
			versionWithMetadata: false,
		},
	}

	for flavor, versions := range flavorToVersion {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s %s", flavor, tc.name), func(t *testing.T) {
				serverVersion := versions["without_metadata"]
				if tc.versionWithMetadata {
					serverVersion = versions["with_metadata"]
				}
				connector := &MySqlConnector{
					serverVersion: serverVersion,
					config: &protos.MySqlConfig{
						Flavor: flavor,
					},
				}
				res, err := connector.mapQValue(tc.qv, tc.field, map[string][]string{
					"size": {"small", "medium", "large"},
				}, tc.versionWithMetadata)
				if tc.err != (err != nil) {
					t.Fatalf("expected error: %v, got: %v", tc.err, err)
				}
				if tc.err {
					return
				}

				require.Equal(t, tc.expected.Value(), res.Value())
			})
		}
	}
}

func TestParseEnumOptions(t *testing.T) {
	testCases := []struct {
		name       string
		columnType string
		expected   []string
	}{
		{
			name:       "single value",
			columnType: "enum('active')",
			expected:   []string{"active"},
		},
		{
			name:       "multiple values",
			columnType: "enum('active','inactive','pending')",
			expected:   []string{"active", "inactive", "pending"},
		},
		{
			name:       "values with spaces",
			columnType: "enum('a b','c d')",
			expected:   []string{"a b", "c d"},
		},
		{
			name:       "numeric-looking values",
			columnType: "enum('1','2','3')",
			expected:   []string{"1", "2", "3"},
		},
		{
			name:       "empty string value",
			columnType: "enum('')",
			expected:   []string{""},
		},
		{
			name:       "value with comma inside",
			columnType: "enum('a,b','c')",
			expected:   []string{"a,b", "c"},
		},
		{
			name:       "multiple values with commas inside",
			columnType: "enum('a,b','c,d,e','f')",
			expected:   []string{"a,b", "c,d,e", "f"},
		},
		{
			name:       "value with escaped single quote",
			columnType: `enum('it''s','fine')`,
			expected:   []string{"its", "fine"},
		},
		{
			name:       "empty string among other values",
			columnType: "enum('active','','pending')",
			expected:   []string{"active", "", "pending"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseEnumOptions(tc.columnType)
			require.Equal(t, tc.expected, result)
		})
	}
}

func testMySQLHost() string {
	if host := os.Getenv("CI_MYSQL_HOST"); host != "" {
		return host
	}
	return "localhost"
}

func setupMySQLConnector(t *testing.T) *MySqlConnector {
	t.Helper()
	var flavor protos.MySqlFlavor
	switch os.Getenv("CI_MYSQL_VERSION") {
	case "maria":
		flavor = protos.MySqlFlavor_MYSQL_MARIA
	default:
		flavor = protos.MySqlFlavor_MYSQL_MYSQL
	}

	connector, err := NewMySqlConnector(t.Context(), &protos.MySqlConfig{
		Host:       testMySQLHost(),
		Port:       3306,
		User:       "root",
		Password:   "cipass",
		DisableTls: true,
		Flavor:     flavor,
	})
	require.NoError(t, err)
	t.Cleanup(func() { connector.Close() })
	return connector
}

func TestGetEnumColumnsInfo(t *testing.T) {
	connector := setupMySQLConnector(t)
	ctx := t.Context()

	dbName := "test_enum_info"
	_, err := connector.Execute(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	require.NoError(t, err)
	_, err = connector.Execute(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = connector.Execute(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	})

	_, err = connector.Execute(ctx, fmt.Sprintf(`CREATE TABLE %s.test_enums (
		id INT AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL,
		priority ENUM('low', 'medium', 'high', 'critical'),
		name VARCHAR(100)
	)`, dbName))
	require.NoError(t, err)

	result, err := connector.GetEnumColumnsInfo(ctx,
		dbName+".test_enums",
		[]string{"status", "priority"},
	)
	require.NoError(t, err)

	require.Equal(t, map[string][]string{
		"status":   {"active", "inactive", "pending"},
		"priority": {"low", "medium", "high", "critical"},
	}, result)
}
