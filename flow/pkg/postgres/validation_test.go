package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsYugabyteVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		want    bool
	}{
		{
			name:    "Yugabyte example 1",
			version: "PostgreSQL 15.11.0-YB-2025.2.2.1-b0 on x86_64-pc-linux-gnu, YugabyteDB, 64-bit",
			want:    true,
		},
		{
			name:    "Yugabyte example 2",
			version: "PostgreSQL 15.12-YB-2025.2.2.2-b0 on x86_64-pc-linux-gnu, compiled by clang version 19.1.0 (https://github.com/yugabyte/llvm-project.git a2a6b655e14e7fa1fcf1011a6cb29cb8575249c0), 64-bit",
			want:    true,
		},
		{
			name:    "Yugabyte example 3",
			version: "PostgreSQL 16-YB-2025.2.2.2-customer-build on x86_64-pc-linux-gnu, compiled by clang version 19.1.0 (https://github.com/yugabyte/llvm-project.git a2a6b655e14e7fa1fcf1011a6cb29cb8575249c0), 64-bit",
			want:    true,
		},
		{
			name:    "Generic Postgres",
			version: "PostgreSQL 15.12 on x86_64-pc-linux-gnu, compiled by gcc, 64-bit",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsYugabyteVersion(tt.version))
		})
	}
}
