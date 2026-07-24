package postgres

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

var yugabyteVersionPattern = regexp.MustCompile(`^PostgreSQL \d+(?:\.\d+)*-YB-\d+(?:\.\d+)*-`)

func IsYugabyteVersion(version string) bool {
	return yugabyteVersionPattern.MatchString(version)
}

func CheckUnsupportedDatabase(ctx context.Context, conn *pgx.Conn) error {
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("failed to get postgres version: %w", err)
	}

	if IsYugabyteVersion(version) {
		return fmt.Errorf("YugabyteDB is not supported")
	}

	return nil
}
