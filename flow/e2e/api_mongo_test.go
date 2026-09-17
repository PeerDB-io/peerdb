//go:build mongodb

package e2e

import (
	"testing"
)

func TestApiMongo(t *testing.T) {
	testApi(t, SetupMongo)
}
