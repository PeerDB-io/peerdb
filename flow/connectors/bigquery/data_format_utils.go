package connbigquery

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"go.temporal.io/sdk/log"
)

const defaultCompressedRowSize = 512 // bytes. Should be a reasonable low default for analytical data

// parquetFooterMagic is the magic bytes at the start and end of a Parquet file (PAR1)
var parquetFooterMagic = [4]byte{'P', 'A', 'R', '1'}

const (
	// minParquetFooterSize is the minimum footer size: 4 bytes for footer length + 4 bytes for magic
	minParquetFooterSize = 8
)

// gcsObjectSeeker implements io.ReadSeeker for a GCS object using range reads.
// Designed for a quick reading specific parts of a Parquet file. Like file header.
// Not efficient for many small reads/seeks.
type gcsObjectSeeker struct {
	ctx context.Context

	object   *storage.ObjectHandle
	fileSize int64
	offset   int64
}

func (r *gcsObjectSeeker) Read(p []byte) (n int, err error) {
	length := int64(len(p))

	if r.offset < 0 || length <= 0 || r.offset+length > r.fileSize {
		return 0, fmt.Errorf("invalid offset/length for reading GCS object chunk: offset=%d, length=%d, fileSize=%d", r.offset, length, r.fileSize)
	}

	reader, err := r.object.NewRangeReader(r.ctx, r.offset, length)
	if err != nil {
		return 0, err
	}

	defer reader.Close()

	n, err = io.ReadFull(reader, p)
	if err != nil {
		return n, err
	}
	r.offset += int64(n)
	return n, nil
}

func (r *gcsObjectSeeker) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.fileSize + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newOffset < 0 || newOffset > r.fileSize {
		return 0, fmt.Errorf("invalid seek offset: %d", newOffset)
	}

	r.offset = newOffset
	return r.offset, nil
}

func readParquetTotalRows(r io.ReadSeeker, fileSize int64) (int64, error) {
	if fileSize < minParquetFooterSize+4 { // 4 bytes header + 8 bytes footer minimum
		return 0, fmt.Errorf("file too small to be a valid parquet file: %d bytes", fileSize)
	}

	if _, err := r.Seek(fileSize-minParquetFooterSize, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to parquet footer: %w", err)
	}
	footerEnd := make([]byte, minParquetFooterSize)
	if _, err := r.Read(footerEnd); err != nil {
		return 0, fmt.Errorf("failed to read parquet footer end: %w", err)
	}

	var magic [4]byte
	copy(magic[:], footerEnd[4:])
	if magic != parquetFooterMagic {
		return 0, fmt.Errorf("invalid parquet file: wrong magic bytes at end")
	}

	// Get footer metadata length (4 bytes, little endian)
	footerMetadataLen := int64(binary.LittleEndian.Uint32(footerEnd[:4]))

	if footerMetadataLen < 0 || footerMetadataLen > fileSize-minParquetFooterSize-4 {
		return 0, fmt.Errorf("footer length out of bounds: %d", footerMetadataLen)
	}

	footerStartOffset := fileSize - minParquetFooterSize - footerMetadataLen

	if _, err := r.Seek(footerStartOffset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to parquet footer: %w", err)
	}
	footerMetadata := make([]byte, footerMetadataLen)
	if _, err := r.Read(footerMetadata); err != nil {
		return 0, fmt.Errorf("failed to read parquet footer end: %w", err)
	}

	fileMetaData, err := metadata.NewFileMetaData(footerMetadata, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to parse parquet file metadata: %w", err)
	}

	return fileMetaData.FileMetaData.NumRows, nil
}

func parquetObjectAverageRowSize(ctx context.Context, logger log.Logger, object *storage.ObjectHandle) uint64 {
	attrs, err := object.Attrs(ctx)
	if err != nil {
		logger.Error("failed to get object attrs for parquet object, using default compressed row size",
			slog.String("object", object.ObjectName()),
			slog.Any("error", err))

		return defaultCompressedRowSize
	}

	r := &gcsObjectSeeker{
		ctx:      ctx,
		object:   object,
		fileSize: attrs.Size,
		offset:   0,
	}

	n, err := readParquetTotalRows(r, attrs.Size)
	if err != nil {
		logger.Error("failed to read parquet object metadata, using default compressed row size",
			slog.String("object", object.ObjectName()),
			slog.Any("error", err))

		return defaultCompressedRowSize
	}

	if n == 0 {
		logger.Info("parquet object has zero rows, using default compressed row size",
			slog.String("object", object.ObjectName()))

		return defaultCompressedRowSize
	}

	return uint64(attrs.Size) / uint64(n)
}
