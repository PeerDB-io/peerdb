package utils

import (
	"fmt"
	"net/url"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func GetPGConnectionString(pgConfig *protos.PostgresConfig) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		pgConfig.User,
		passwordEscaped,
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.Database,
	)
	return connString
}
