package internal

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func ClickHouseTestHost() string {
	return GetEnvString("CI_CLICKHOUSE_HOST", "localhost")
}

func ClickHouseTestPort() uint32 {
	return uint32(getEnvUint[uint16]("CI_CLICKHOUSE_NATIVE_PORT", 9000))
}

func MySQLTestHostWithFallback(fallback string) string {
	return GetEnvString("CI_MYSQL_HOST", fallback)
}

func MySQLTestHost() string {
	return MySQLTestHostWithFallback("localhost")
}

func MySQLTestPortWithFallback(fallback uint32) uint32 {
	envPortStr, ok := os.LookupEnv("CI_MYSQL_PORT")
	if !ok {
		return fallback
	}
	envPort, err := strconv.ParseUint(strings.TrimSpace(strings.Split(envPortStr, "#")[0]), 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse CI_MYSQL_PORT: %v", err))
	}
	return uint32(envPort)
}

func MySQLTestPort() uint32 {
	return MySQLTestPortWithFallback(3306)
}

func MySQLTestVersion() string {
	return os.Getenv("CI_MYSQL_VERSION")
}

func MySQLTestVersionIsMaria() bool {
	return MySQLTestVersion() == "maria"
}

func MySQLTestVersionIsMysqlPos() bool {
	return MySQLTestVersion() == "mysql-pos"
}

func SetupRDSIAMAuthAWSCredentials(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_ACCESS_KEY_ID"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SECRET_ACCESS_KEY"))
	t.Setenv("AWS_SESSION_TOKEN", os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_AWS_SESSION_TOKEN"))
}

func RDSIAMAuthAssumeRoleConfig() *protos.AwsAuthenticationConfig {
	return &protos.AwsAuthenticationConfig{
		AuthType: protos.AwsIAMAuthConfigType_IAM_AUTH_ASSUME_ROLE,
		AuthConfig: &protos.AwsAuthenticationConfig_Role{
			Role: &protos.AWSAuthAssumeRoleConfig{
				AssumeRoleArn:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE"),
				ChainedRoleArn: new(os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_CHAINED_ROLE")),
			},
		},
	}
}

type RDSIAMAuthTestConnectionInfo struct {
	Host      string
	ProxyHost string
	Username  string
}

func RDSIAMAuthPostgresTestConnectionInfo() RDSIAMAuthTestConnectionInfo {
	return RDSIAMAuthTestConnectionInfo{
		Host:      os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES"),
		ProxyHost: os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES_PROXY"),
		Username:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_POSTGRES"),
	}
}

func RDSIAMAuthMySQLTestConnectionInfo() RDSIAMAuthTestConnectionInfo {
	return RDSIAMAuthTestConnectionInfo{
		Host:      os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL"),
		ProxyHost: os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL_PROXY"),
		Username:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_MYSQL"),
	}
}

type MongoTestCredentials struct {
	URI      string
	Username string
	Password string
}

func MongoAdminTestCredentials(t *testing.T) MongoTestCredentials {
	t.Helper()
	creds := MongoTestCredentials{
		URI:      os.Getenv("CI_MONGO_ADMIN_URI"),
		Username: os.Getenv("CI_MONGO_ADMIN_USERNAME"),
		Password: os.Getenv("CI_MONGO_ADMIN_PASSWORD"),
	}
	require.NotEmpty(t, creds.URI, "missing CI_MONGO_ADMIN_URI env var")
	require.NotEmpty(t, creds.Username, "missing CI_MONGO_ADMIN_USERNAME env var")
	require.NotEmpty(t, creds.Password, "missing CI_MONGO_ADMIN_PASSWORD env var")
	return creds
}

func MongoUserTestCredentials(t *testing.T) MongoTestCredentials {
	t.Helper()
	creds := MongoTestCredentials{
		URI:      os.Getenv("CI_MONGO_URI"),
		Username: os.Getenv("CI_MONGO_USERNAME"),
		Password: os.Getenv("CI_MONGO_PASSWORD"),
	}
	require.NotEmpty(t, creds.URI, "missing CI_MONGO_URI env var")
	require.NotEmpty(t, creds.Username, "missing CI_MONGO_USERNAME env var")
	require.NotEmpty(t, creds.Password, "missing CI_MONGO_PASSWORD env var")
	return creds
}
