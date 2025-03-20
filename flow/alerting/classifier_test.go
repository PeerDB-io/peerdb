package alerting

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestPostgresDNSErrorShouldBeConnectivity(t *testing.T) {
	config, err := pgx.ParseConfig("postgres://non-existent.domain.name.here:123/db")
	assert.NoError(t, err)
	_, err = pgx.ConnectConfig(t.Context(), config)
	errorClass, errInfo := GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceNet,
		Code:   "net.DNSError",
	}, errInfo, "Unexpected error info")
}
