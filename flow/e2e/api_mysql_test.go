package e2e

import (
	"testing"
)

func TestApiMy(t *testing.T) {
	testApi(t, SetupMySQL)
}

func TestApiMariaDB(t *testing.T) {
	testApi(t, SetupMariaDB)
}
