//go:build postgres

package e2e

import (
	"testing"
)

func TestApiPg(t *testing.T) {
	testApi(t, SetupPostgres)
}
