package peerdbenv

import "time"

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
