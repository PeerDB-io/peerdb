package peerdbenv

import (
	"time"
)

// This file contains functions to get the values of various peerdb environment
// variables. This will help catalog the environment variables that are used
// throughout the codebase.

// PEERDB_VERSION_SHA_SHORT
func GetPeerDBVersionShaShort() string {
	return getEnvString("PEERDB_VERSION_SHA_SHORT", "unknown")
}

// PEERDB_DEPLOYMENT_UID
func GetPeerDBDeploymentUID() string {
	return getEnvString("PEERDB_DEPLOYMENT_UID", "")
}

// PEERDB_CDC_CHANNEL_BUFFER_SIZE
func GetPeerDBCDCChannelBufferSize() int {
	return getEnvInt("PEERDB_CDC_CHANNEL_BUFFER_SIZE", 1<<18)
}

// PEERDB_EVENTHUB_FLUSH_TIMEOUT_SECONDS
func GetPeerDBEventhubFlushTimeoutSeconds() time.Duration {
	x := getEnvInt("PEERDB_EVENTHUB_FLUSH_TIMEOUT_SECONDS", 10)
	return time.Duration(x) * time.Second
}

// PEERDB_CDC_IDLE_TIMEOUT_SECONDS
func GetPeerDBCDCIdleTimeoutSeconds() time.Duration {
	x := getEnvInt("PEERDB_CDC_IDLE_TIMEOUT_SECONDS", 60)
	return time.Duration(x) * time.Second
}

// PEERDB_CDC_DISK_SPILL_THRESHOLD
func GetPeerDBCDCDiskSpillThreshold() int {
	return getEnvInt("PEERDB_CDC_DISK_SPILL_THRESHOLD", 1_000_000)
}

// PEERDB_CATALOG_HOST
func GetPeerDBCatalogHost() string {
	return getEnvString("PEERDB_CATALOG_HOST", "")
}

// PEERDB_CATALOG_PORT
func GetPeerDBCatalogPort() uint32 {
	return getEnvUint32("PEERDB_CATALOG_PORT", 5432)
}

// PEERDB_CATALOG_USER
func GetPeerDBCatalogUser() string {
	return getEnvString("PEERDB_CATALOG_USER", "")
}

// PEERDB_CATALOG_PASSWORD
func GetPeerDBCatalogPassword() string {
	return getEnvString("PEERDB_CATALOG_PASSWORD", "")
}

// PEERDB_CATALOG_DATABASE
func GetPeerDBCatalogDatabase() string {
	return getEnvString("PEERDB_CATALOG_DATABASE", "")
}

// PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD, 0 disables slot lag alerting entirely
func GetPeerDBSlotLagMBAlertThreshold() uint32 {
	return getEnvUint32("PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD", 5000)
}

// PEERDB_ALERTING_GAP_MINUTES, 0 disables all alerting entirely
func GetPeerDBAlertingGapMinutesAsDuration() time.Duration {
	why := time.Duration(getEnvUint32("PEERDB_ALERTING_GAP_MINUTES", 15))
	return why * time.Minute
}

// PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD, 0 disables open connections alerting entirely
func GetPeerDBOpenConnectionsAlertThreshold() uint32 {
	return getEnvUint32("PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD", 5)
}
