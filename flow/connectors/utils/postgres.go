package utils

import (
	"fmt"
	"net/url"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func GetPGConnectionString(pgConfig *protos.PostgresConfig) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		pgConfig.Host, pgConfig.Port, pgConfig.User, passwordEscaped, pgConfig.Database)
	return connString
}
