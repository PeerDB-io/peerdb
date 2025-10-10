package connbigquery

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"cloud.google.com/go/storage"
	"github.com/hamba/avro/v2/ocf"
	"go.temporal.io/sdk/log"
)

// avroTotalRows returns total number of rows in Avro file from the reader.
func avroTotalRows(ctx context.Context, r io.Reader) (uint64, error) {
	decoder, err := ocf.NewDecoder(r)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF decoder: %w", err)
	}

	var totalRows uint64
	for decoder.HasNext() {
		if err := ctx.Err(); err != nil {
			return totalRows, err
		}

		// Decode into a generic map to avoid needing to know the exact schema
		var record map[string]interface{}
		if err := decoder.Decode(&record); err != nil {
			return totalRows, fmt.Errorf("failed to decode record: %w", err)
		}
		totalRows++
	}

	if err := decoder.Error(); err != nil {
		return totalRows, fmt.Errorf("decoder error: %w", err)
	}

	return totalRows, nil
}

const defaultCompressedRowSize = 32 * 1024 // 32KB

func avroObjectAverageRowSize(ctx context.Context, logger log.Logger, object *storage.ObjectHandle) uint64 {
	r, err := object.NewReader(ctx)
	if err != nil {
		logger.Error("failed to create reader for avro object, using default compressed row size",
			slog.String("object", object.ObjectName()),
			slog.Any("error", err))

		return defaultCompressedRowSize
	}
	defer func() {
		_ = r.Close()
	}()

	n, err := avroTotalRows(ctx, r)
	if err != nil {
		logger.Error("failed to read avro object, using default compressed row size",
			slog.String("object", object.ObjectName()),
			slog.Any("error", err))

		return defaultCompressedRowSize
	}

	if n == 0 {
		logger.Info("avro object has zero rows, using default compressed row size",
			slog.String("object", object.ObjectName()))

		return defaultCompressedRowSize
	}

	return uint64(r.Attrs.Size) / n
}
