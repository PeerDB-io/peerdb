package connclickhouse

import (
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

func TestIsRetryableGCSError(t *testing.T) {
	t.Run("nil error returns false", func(t *testing.T) {
		require.False(t, isRetryableGCSError(nil))
	})

	t.Run("non-clickhouse error returns false", func(t *testing.T) {
		require.False(t, isRetryableGCSError(errors.New("some random error")))
	})

	t.Run("clickhouse error with wrong code returns false", func(t *testing.T) {
		err := &clickhouse.Exception{
			Code:    60, // Unknown table error
			Message: "Table does not exist",
		}
		require.False(t, isRetryableGCSError(err))
	})

	t.Run("clickhouse error code 86 but not GCS returns false", func(t *testing.T) {
		err := &clickhouse.Exception{
			Code:    86,
			Message: "Received error from remote server https://example.com/data.parquet. HTTP status code: 401 'Unauthorized'",
		}
		require.False(t, isRetryableGCSError(err))
	})

	t.Run("clickhouse error code 86 GCS but not 401 returns false", func(t *testing.T) {
		err := &clickhouse.Exception{
			Code:    86,
			Message: "Received error from remote server https://storage.googleapis.com/bucket/file.parquet. HTTP status code: 403 'Forbidden'",
		}
		require.False(t, isRetryableGCSError(err))
	})

	t.Run("GCS 401 error returns true", func(t *testing.T) {
		err := &clickhouse.Exception{
			Code:    86,
			Message: `Received error from remote server https://storage.googleapis.com/xxxx/dddd%2Fccc.53bc2bcae41e2d9c662e1ba7%2F000000163894.parquet. HTTP status code: 401 'Unauthorized', body length: 131 bytes, body: '<?xml version='1.0' encoding='UTF-8'?><Error><Code>AuthenticationRequired</Code><Message>Authentication required.</Message></Error>': (in file/uri https://storage.googleapis.com/xxxx/dddd%2Fccc.53bc2bcae41e2d9c662e1ba7%2F000000163894.parquet): While executing ParquetBlockInputFormat: While executing URL`,
		}
		require.True(t, isRetryableGCSError(err))
	})

	t.Run("GCS 401 error with different bucket returns true", func(t *testing.T) {
		err := &clickhouse.Exception{
			Code:    86,
			Message: "Received error from remote server https://storage.googleapis.com/another-bucket/path/to/file.parquet. HTTP status code: 401 'Unauthorized'",
		}
		require.True(t, isRetryableGCSError(err))
	})
}
