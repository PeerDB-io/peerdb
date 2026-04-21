package internal

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func ClickHouseTestHost() string {
	return GetEnvString("CI_CLICKHOUSE_HOST", "localhost")
}

func ClickHouseTestPort() uint32 {
	return uint32(getEnvUint[uint16]("CI_CLICKHOUSE_NATIVE_PORT", 9000))
}

func MySQLTestHost() string {
	return GetEnvString("CI_MYSQL_HOST", "localhost")
}

func MySQLTestPort() uint32 {
	envPortStr, ok := os.LookupEnv("CI_MYSQL_PORT")
	if !ok {
		return 3306
	}
	envPort, err := strconv.ParseUint(strings.TrimSpace(strings.Split(envPortStr, "#")[0]), 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse CI_MYSQL_PORT: %v", err))
	}
	return uint32(envPort)
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
