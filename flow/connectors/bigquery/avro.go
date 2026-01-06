package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"cloud.google.com/go/storage"
	"github.com/hamba/avro/v2"
	"go.temporal.io/sdk/log"
)

var ocfMagicBytes = [4]byte{'O', 'b', 'j', 1}

// avroTotalRows returns total number of rows in Avro file from the reader.
// This is a lightweight implementation that counts rows by reading OCF block headers
// without decoding the actual record data.
func avroTotalRows(ctx context.Context, r io.Reader) (uint64, error) {
	reader := avro.NewReader(r, 1024)

	// Read and validate header
	sync, err := readOCFHeader(reader)
	if err != nil {
		return 0, fmt.Errorf("failed to read OCF header: %w", err)
	}

	// Count rows by summing block counts
	var totalRows uint64
	for {
		if err := ctx.Err(); err != nil {
			return totalRows, err
		}

		count, done, err := readBlockCount(reader, sync)
		if err != nil {
			return totalRows, fmt.Errorf("failed to read block: %w", err)
		}
		if done {
			break
		}
		totalRows += uint64(count)
	}

	return totalRows, nil
}

// readOCFHeader reads the OCF file header and returns the sync marker.
func readOCFHeader(reader *avro.Reader) ([16]byte, error) {
	var magic [4]byte
	reader.Read(magic[:])
	if reader.Error != nil {
		return [16]byte{}, fmt.Errorf("failed to read magic bytes: %w", reader.Error)
	}
	if magic != ocfMagicBytes {
		return [16]byte{}, errors.New("invalid avro file: wrong magic bytes")
	}

	// Skip metadata map
	mapLen := reader.ReadLong()
	for mapLen != 0 {
		if mapLen < 0 {
			mapLen = -mapLen
			_ = reader.ReadLong() // skip byte count
		}
		for range mapLen {
			_ = reader.ReadString() // key
			_ = reader.ReadBytes()  // value
		}
		mapLen = reader.ReadLong()
	}
	if reader.Error != nil {
		return [16]byte{}, fmt.Errorf("failed to read metadata: %w", reader.Error)
	}

	// Read sync marker
	var sync [16]byte
	reader.Read(sync[:])
	if reader.Error != nil {
		return [16]byte{}, fmt.Errorf("failed to read sync marker: %w", reader.Error)
	}

	return sync, nil
}

// readBlockCount reads the next block's count and skips its data.
// Returns the count, whether we've reached EOF, and any error.
func readBlockCount(reader *avro.Reader, expectedSync [16]byte) (int64, bool, error) {
	// Check for EOF
	_ = reader.Peek()
	if errors.Is(reader.Error, io.EOF) {
		return 0, true, nil
	}

	count := reader.ReadLong()
	if errors.Is(reader.Error, io.EOF) {
		return 0, true, nil
	}

	size := reader.ReadLong()
	if reader.Error != nil {
		return 0, false, fmt.Errorf("failed to read block header: %w", reader.Error)
	}

	// Skip the block data
	reader.SkipNBytes(int(size))
	if reader.Error != nil {
		return 0, false, fmt.Errorf("failed to skip block data: %w", reader.Error)
	}

	// Read and verify sync marker
	var sync [16]byte
	reader.Read(sync[:])
	if reader.Error != nil && !errors.Is(reader.Error, io.EOF) {
		return 0, false, fmt.Errorf("failed to read sync marker: %w", reader.Error)
	}
	if sync != expectedSync && !errors.Is(reader.Error, io.EOF) {
		return 0, false, errors.New("invalid block: sync marker mismatch")
	}

	return count, false, nil
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
