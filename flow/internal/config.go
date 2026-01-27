package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// This file contains functions to get the values of various peerdb environment
// variables. This will help catalog the environment variables that are used
// throughout the codebase.

// PEERDB_VERSION_SHA_SHORT
func PeerDBVersionShaShort() string {
	return GetEnvString("PEERDB_VERSION_SHA_SHORT", "unknown")
}

// PEERDB_DEPLOYMENT_UID
func PeerDBDeploymentUID() string {
	return GetEnvString("PEERDB_DEPLOYMENT_UID", "")
}

func PeerDBDeploymentVersion() string {
	return GetEnvString("PEERDB_DEPLOYMENT_VERSION", "")
}

func PeerFlowTaskQueueName(taskQueueID shared.TaskQueueID) string {
	deploymentUID := PeerDBDeploymentUID()
	if deploymentUID == "" {
		return string(taskQueueID)
	}
	return fmt.Sprintf("%s-%s", deploymentUID, taskQueueID)
}

// GOMEMLIMIT is a variable internal to Golang itself, we use this for internal targets, 0 means no maximum
func PeerDBFlowWorkerMaxMemBytes() uint64 {
	return getEnvUint[uint64]("GOMEMLIMIT", 0)
}

// PEERDB_CATALOG_HOST
func PeerDBCatalogHost() string {
	return GetEnvString("PEERDB_CATALOG_HOST", "")
}

// PEERDB_CATALOG_PORT
func PeerDBCatalogPort() uint16 {
	return getEnvUint[uint16]("PEERDB_CATALOG_PORT", 5432)
}

// PEERDB_CATALOG_USER
func PeerDBCatalogUser() string {
	return GetEnvString("PEERDB_CATALOG_USER", "")
}

// PEERDB_CATALOG_PASSWORD
func PeerDBCatalogPassword(ctx context.Context) string {
	val, err := GetKmsDecryptedEnvString(ctx, "PEERDB_CATALOG_PASSWORD", "")
	if err != nil {
		slog.ErrorContext(ctx, "failed to decrypt PEERDB_CATALOG_PASSWORD", slog.Any("error", err))
		panic(err)
	}

	return val
}

// PEERDB_CATALOG_DATABASE
func PeerDBCatalogDatabase() string {
	return GetEnvString("PEERDB_CATALOG_DATABASE", "")
}

func PeerDBCatalogRequireTls() bool {
	return getEnvConvert("PEERDB_CATALOG_REQUIRE_TLS", false, strconv.ParseBool)
}

// PEERDB_TELEMETRY_AWS_SNS_TOPIC_ARN
func PeerDBTelemetryAWSSNSTopicArn() string {
	return GetEnvString("PEERDB_TELEMETRY_AWS_SNS_TOPIC_ARN", "")
}

func PeerDBAlertingEmailSenderSourceEmail() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_SOURCE_EMAIL", "")
}

func PeerDBAlertingEmailSenderConfigurationSet() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_CONFIGURATION_SET", "")
}

func PeerDBAlertingEmailSenderRegion() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_REGION", "")
}

// Comma-separated reply-to addresses
func PeerDBAlertingEmailSenderReplyToAddresses() string {
	return GetEnvString("PEERDB_ALERTING_EMAIL_SENDER_REPLY_TO_ADDRESSES", "")
}

func PeerDBCurrentEncKeyID() string {
	return GetEnvString("PEERDB_CURRENT_ENC_KEY_ID", "")
}

func PeerDBEncKeys(ctx context.Context) shared.PeerDBEncKeys {
	val, err := GetKmsDecryptedEnvString(ctx, "PEERDB_ENC_KEYS", "")
	if err != nil {
		slog.ErrorContext(ctx, "failed to decrypt PEERDB_ENC_KEYS", slog.Any("error", err))
		panic(err)
	}

	var result shared.PeerDBEncKeys
	if err := json.Unmarshal([]byte(val), &result); err != nil {
		return nil
	}

	return result
}

func PeerDBCurrentEncKey(ctx context.Context) (shared.PeerDBEncKey, error) {
	encKeyID := PeerDBCurrentEncKeyID()
	encKeys := PeerDBEncKeys(ctx)
	return encKeys.Get(encKeyID)
}

func PeerDBAllowedTargets() string {
	return GetEnvString("PEERDB_ALLOWED_TARGETS", "")
}

func PeerDBOnlyClickHouseAllowed() bool {
	return strings.EqualFold(PeerDBAllowedTargets(), protos.DBType_CLICKHOUSE.String())
}

func PeerDBClickHouseAllowedDomains() string {
	return GetEnvString("PEERDB_CLICKHOUSE_ALLOWED_DOMAINS", "")
}

func PeerDBTemporalEnableCertAuth() bool {
	cert := GetEnvString("TEMPORAL_CLIENT_CERT", "")
	return strings.TrimSpace(cert) != ""
}

func PeerDBTemporalClientCert(ctx context.Context) ([]byte, error) {
	return GetKmsDecryptedEnvBase64EncodedBytes(ctx, "TEMPORAL_CLIENT_CERT", nil)
}

func PeerDBTemporalClientKey(ctx context.Context) ([]byte, error) {
	return GetKmsDecryptedEnvBase64EncodedBytes(ctx, "TEMPORAL_CLIENT_KEY", nil)
}

func PeerDBTemporalClientCertPath() string {
	return GetEnvString("TEMPORAL_CLIENT_CERT_PATH", "")
}

func PeerDBTemporalClientKeyPath() string {
	return GetEnvString("TEMPORAL_CLIENT_KEY_PATH", "")
}

func PeerDBGetIncidentIoUrl() string {
	return GetEnvString("PEERDB_INCIDENTIO_URL", "")
}

func PeerDBGetIncidentIoToken() string {
	return GetEnvString("PEERDB_INCIDENTIO_TOKEN", "")
}

func PeerDBRAPIRequestLoggingEnabled(ctx context.Context) bool {
	requestLoggingEnabled, err := strconv.ParseBool(GetEnvString("PEERDB_API_REQUEST_LOGGING_ENABLED", "false"))
	if err != nil {
		slog.ErrorContext(ctx, "failed to parse PEERDB_API_REQUEST_LOGGING_ENABLED to bool", slog.Any("error", err))
		return false
	}
	return requestLoggingEnabled
}

// PEERDB_MAINTENANCE_MODE_WAIT_ALERT_SECONDS is how long to wait before alerting that peerdb's been stuck in maintenance mode too long
func PeerDBMaintenanceModeWaitAlertSeconds() int {
	return getEnvConvert("PEERDB_MAINTENANCE_MODE_WAIT_ALERT_SECONDS", 600, strconv.Atoi)
}

// PEERDB_TELEMETRY_SENDER_SEND_ERROR_ALERTS_ENABLED is whether to send error alerts to the telemetry sender
func PeerDBTelemetrySenderSendErrorAlertsEnabled(ctx context.Context) bool {
	enabled, err := strconv.ParseBool(GetEnvString("PEERDB_TELEMETRY_SENDER_SEND_ERROR_ALERTS_ENABLED", "false"))
	if err != nil {
		slog.ErrorContext(ctx, "failed to parse PEERDB_TELEMETRY_SENDER_SEND_ERROR_ALERTS_ENABLED to bool", slog.Any("error", err))
		return false
	}
	return enabled
}
