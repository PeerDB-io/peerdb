package internal

import (
	"net/url"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// This function determines when PG should use TLS connections in function of the peer configuration.
// The logic is as follows:
// 1. If TLS is explicitly required by the user (RequireTls=true), then we use TLS.
// 2. If TLS is not explicitly required, but DisableTls is explicitly set to false, then we also use TLS.
// 3. Otherwise, we do not use TLS.
// Cloud SQL IAM connections never negotiate TLS with Postgres: the Cloud SQL connector encrypts them itself.
func PGMustUseTlsConnection(pgConfig *protos.PostgresConfig) bool {
	if pgConfig.AuthType == protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH {
		return false
	}
	return pgConfig.RequireTls ||
		(pgConfig.DisableTls != nil && !*pgConfig.DisableTls)
}

// SanitizePGHost strips pasted connection-string junk (path/query suffixes,
// whitespace, IPv6 brackets) from a stored host, yielding a bare hostname or IP.
func SanitizePGHost(host string) string {
	host = strings.TrimSpace(host)
	host, _, _ = strings.Cut(host, "/")
	host, _, _ = strings.Cut(host, "?")
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	return host
}

// CloudSQLConnectorPlaceholderHost stands in for the host of Cloud SQL IAM connections in connection strings:
// an instance connection name (project:region:instance) does not parse as a URL host, and the Cloud SQL connector
// ignores the host and port it is asked to dial.
const CloudSQLConnectorPlaceholderHost = "cloudsql-connector"

func GetPGConnectionString(pgConfig *protos.PostgresConfig, flowName string) string {
	host := SanitizePGHost(pgConfig.Host)
	port := pgConfig.Port
	isCloudSQLIAM := pgConfig.AuthType == protos.PostgresAuthType_POSTGRES_GCP_CLOUD_SQL_IAM_AUTH
	if isCloudSQLIAM {
		host = CloudSQLConnectorPlaceholderHost
		// the connector ignores the port, but the connection string still has to parse
		// when a peer was created without one
		port = 5432
	}

	u := &url.URL{
		Scheme: "postgres",
		Host:   shared.JoinHostPort(host, port),
		Path:   "/" + pgConfig.Database,
	}

	if pgConfig.Password != "" {
		u.User = url.UserPassword(pgConfig.User, pgConfig.Password)
	} else {
		u.User = url.User(pgConfig.User)
	}

	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	q := u.Query()
	q.Set("application_name", applicationName)
	q.Set("client_encoding", "UTF8")
	if isCloudSQLIAM {
		q.Set("sslmode", "disable")
	} else if PGMustUseTlsConnection(pgConfig) {
		q.Set("sslmode", "require")
		// When require or more strict modes are set, the PostgreSQL client library will use the TLS
		// configuration provided by the user.
		// Other modes such as "verify-ca" or "verify-full" might be relaxed by this configuration.
	}

	// Preserve spaces in application_name; pgx treats '+' literally.
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")

	return u.String()
}
