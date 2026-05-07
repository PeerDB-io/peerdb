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

func GetAncillaryPostgresConfigFromEnv() *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:     GetEnvString("PG_HOST", "localhost"),
		Port:     uint32(getEnvUint[uint16]("PG_PORT", 5432)),
		User:     GetEnvString("PG_USER", "postgres"),
		Password: GetEnvString("PG_PASSWORD", "postgres"),
		Database: GetEnvString("PG_DATABASE", "postgres"),
	}
}

func GetSecondaryPostgresConfigFromEnv() *protos.PostgresConfig {
	return &protos.PostgresConfig{
		Host:     GetEnvString("PG2_HOST", "localhost"),
		Port:     uint32(getEnvUint[uint16]("PG2_PORT", 5437)),
		User:     GetEnvString("PG2_USER", "postgres"),
		Password: GetEnvString("PG2_PASSWORD", "postgres"),
		Database: GetEnvString("PG2_DATABASE", "postgres"),
	}
}

func PostgresToxiproxyUpstreamHostWithFallback(fallback string) string {
	return GetEnvString("TOXIPROXY_POSTGRES_HOST", fallback)
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

func MySQLTestRootPasswordWithFallback(fallback string) string {
	return GetEnvString("CI_MYSQL_ROOT_PASSWORD", fallback)
}

func MySQLTestVersion() string {
	return os.Getenv("CI_MYSQL_VERSION")
}

func MySQLTestVersionIsMaria() bool {
	return MySQLTestVersion() == "maria"
}

func MySQLTestVersionIsMySQLPos() bool {
	return MySQLTestVersion() == "mysql-pos"
}

// setupAWSCredsFromEnv copies the three AWS_* credential env vars from sources
// named "<sourcePrefix>AWS_ACCESS_KEY_ID" etc. into the unprefixed AWS_* names
// for the duration of the test. Each source must be non-empty.
func setupAWSCredsFromEnv(t *testing.T, sourcePrefix string) {
	t.Helper()
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		source := sourcePrefix + name
		value := os.Getenv(source)
		require.NotEmpty(t, value, "missing "+source+" env var")
		t.Setenv(name, value)
	}
}

func SetupFlowAWSCredentialsFromEnv(t *testing.T) {
	t.Helper()
	setupAWSCredsFromEnv(t, "FLOW_TESTS_")
}

func SetupRDSIAMAuthAWSCredentials(t *testing.T) {
	t.Helper()
	setupAWSCredsFromEnv(t, "FLOW_TESTS_RDS_IAM_AUTH_")
}

func RDSIAMAuthAssumeRoleConfig(t *testing.T) *protos.AwsAuthenticationConfig {
	t.Helper()
	assumeRoleArn := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE")
	require.NotEmpty(t, assumeRoleArn, "missing FLOW_TESTS_RDS_IAM_AUTH_ASSUME_ROLE env var")
	chainedRoleArnEnv := os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_CHAINED_ROLE") // Optional
	var maybeChainedRoleArn *string
	if chainedRoleArnEnv != "" {
		maybeChainedRoleArn = &chainedRoleArnEnv
	}
	return &protos.AwsAuthenticationConfig{
		AuthType: protos.AwsIAMAuthConfigType_IAM_AUTH_ASSUME_ROLE,
		AuthConfig: &protos.AwsAuthenticationConfig_Role{
			Role: &protos.AWSAuthAssumeRoleConfig{
				AssumeRoleArn:  assumeRoleArn,
				ChainedRoleArn: maybeChainedRoleArn,
			},
		},
	}
}

type RDSIAMAuthTestConnectionInfo struct {
	Host      string
	ProxyHost string
	Username  string
}

func RDSIAMAuthPostgresTestConnectionInfo(t *testing.T) RDSIAMAuthTestConnectionInfo {
	t.Helper()
	info := RDSIAMAuthTestConnectionInfo{
		Host:      os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES"),
		ProxyHost: os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES_PROXY"),
		Username:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_POSTGRES"),
	}
	require.NotEmpty(t, info.Host, "missing FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES env var")
	require.NotEmpty(t, info.ProxyHost, "missing FLOW_TESTS_RDS_IAM_AUTH_HOST_POSTGRES_PROXY env var")
	require.NotEmpty(t, info.Username, "missing FLOW_TESTS_RDS_IAM_AUTH_USERNAME_POSTGRES env var")
	return info
}

func RDSIAMAuthMySQLTestConnectionInfo(t *testing.T) RDSIAMAuthTestConnectionInfo {
	t.Helper()
	info := RDSIAMAuthTestConnectionInfo{
		Host:      os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL"),
		ProxyHost: os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL_PROXY"),
		Username:  os.Getenv("FLOW_TESTS_RDS_IAM_AUTH_USERNAME_MYSQL"),
	}
	require.NotEmpty(t, info.Host, "missing FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL env var")
	require.NotEmpty(t, info.ProxyHost, "missing FLOW_TESTS_RDS_IAM_AUTH_HOST_MYSQL_PROXY env var")
	require.NotEmpty(t, info.Username, "missing FLOW_TESTS_RDS_IAM_AUTH_USERNAME_MYSQL env var")
	return info
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
