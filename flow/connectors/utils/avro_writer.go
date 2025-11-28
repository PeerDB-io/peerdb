package utils

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type (
	AvroStorageLocation int64
)

const (
	AvroLocalStorage = iota
	AvroS3Storage
	AvroGCSStorage
)

type peerDBOCFWriter struct {
	stream               *model.QRecordStream
	avroSchema           *model.QRecordAvroSchemaDefinition
	sizeTracker          *model.QRecordAvroChunkSizeTracker
	avroCompressionCodec ocf.CodecName
	targetDWH            protos.DBType
}

type AvroFile struct {
	FilePath        string              `json:"filePath"`
	StorageLocation AvroStorageLocation `json:"storageLocation"`
	NumRecords      int64               `json:"numRecords"`
}

func (l *AvroFile) Cleanup(ctx context.Context) {
	if l.StorageLocation == AvroLocalStorage {
		if err := os.Remove(l.FilePath); err != nil && !os.IsNotExist(err) {
			slog.WarnContext(ctx, "unable to delete temporary Avro file", slog.Any("error", err))
		}
	}
}

func NewPeerDBOCFWriter(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	avroCompressionCodec ocf.CodecName,
	targetDWH protos.DBType,
	sizeTracker *model.QRecordAvroChunkSizeTracker,
) *peerDBOCFWriter {
	return &peerDBOCFWriter{
		stream:               stream,
		avroSchema:           avroSchema,
		avroCompressionCodec: avroCompressionCodec,
		targetDWH:            targetDWH,
		sizeTracker:          sizeTracker,
	}
}

func (p *peerDBOCFWriter) WriteOCF(
	ctx context.Context,
	env map[string]string,
	w io.Writer,
	typeConversions map[string]types.TypeConversion,
	numericTruncator model.SnapshotTableNumericTruncator,
) (int64, error) {
	ocfWriter, err := ocf.NewEncoderWithSchema(
		p.avroSchema.Schema, w, ocf.WithCodec(p.avroCompressionCodec),
		ocf.WithBlockLength(8192), ocf.WithBlockSize(1<<26),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}
	defer ocfWriter.Close()

	numRows, err := p.writeRecordsToOCFWriter(ctx, env, ocfWriter, typeConversions, numericTruncator)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to OCF writer: %w", err)
	}
	return numRows, nil
}

func (p *peerDBOCFWriter) WriteRecordsToS3(
	ctx context.Context,
	env map[string]string,
	bucketName string,
	key string,
	s3Creds AWSCredentialsProvider,
	typeConversions map[string]types.TypeConversion,
	numericTruncator model.SnapshotTableNumericTruncator,
) (AvroFile, error) {
	logger := internal.LoggerFromCtx(ctx)
	s3svc, err := CreateS3Client(ctx, s3Creds)
	if err != nil {
		logger.Error("failed to create S3 client", slog.Any("error", err))
		return AvroFile{}, fmt.Errorf("failed to create S3 client: %w", err)
	}

	r, w := io.Pipe()
	defer r.Close()

	var writeOcfError error
	var numRows int64

	go func() {
		defer func() {
			if r := recover(); r != nil {
				writeOcfError = fmt.Errorf("panic occurred during WriteOCF: %v", r)
				stack := string(debug.Stack())
				logger.Error("panic during WriteOCF", slog.Any("error", writeOcfError), slog.String("stack", stack))
			}
			w.Close()
		}()
		var writer io.Writer
		if p.sizeTracker == nil || p.sizeTracker.TrackUncompressed {
			writer = w
		} else {
			writer = shared.NewWatchWriter(w, &p.sizeTracker.Bytes)
		}
		numRows, writeOcfError = p.WriteOCF(ctx, env, writer, typeConversions, numericTruncator)
	}()

	partSize, err := internal.PeerDBS3PartSize(ctx, env)
	if err != nil {
		return AvroFile{}, fmt.Errorf("could not get s3 part size config: %w", err)
	}

	// Create the uploader using the AWS SDK v2 manager
	uploader := manager.NewUploader(s3svc, func(u *manager.Uploader) {
		if partSize > 0 {
			u.PartSize = partSize
			if partSize > 256*1024*1024 {
				u.Concurrency = 1
			}
		}
	})

	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   r,
	}); err != nil {
		s3Path := "s3://" + bucketName + "/" + key
		logger.Error("failed to upload file", slog.Any("error", err), slog.String("s3_path", s3Path))
		return AvroFile{}, fmt.Errorf("failed to upload file: %w", err)
	}

	if writeOcfError != nil {
		logger.Error("failed to write records to OCF", slog.Any("error", writeOcfError))
		return AvroFile{}, writeOcfError
	}

	logger.Info("finished s3 upload")

	return AvroFile{
		StorageLocation: AvroS3Storage,
		FilePath:        key,
		NumRecords:      numRows,
	}, nil
}

func (p *peerDBOCFWriter) WriteRecordsToAvroFile(ctx context.Context, env map[string]string, filePath string) (AvroFile, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return AvroFile{}, fmt.Errorf("failed to create temporary Avro file: %w", err)
	}
	defer file.Close()
	printFileStats := func(message string) {
		logger := internal.LoggerFromCtx(ctx)
		stats, err := file.Stat()
		if err != nil {
			return
		}
		logger.Info(message, slog.String("file", filePath), slog.Int64("size", stats.Size()))
	}
	shutdown := shared.Interval(ctx, time.Minute, func() { printFileStats("writing to temporary Avro file") })
	defer shutdown()

	numRecords, err := p.WriteOCF(ctx, env, file, nil, nil)
	if err != nil {
		return AvroFile{}, fmt.Errorf("failed to write records to temporary Avro file: %w", err)
	}

	printFileStats("finished writing to temporary Avro file")
	return AvroFile{
		NumRecords:      numRecords,
		StorageLocation: AvroLocalStorage,
		FilePath:        filePath,
	}, nil
}

func (p *peerDBOCFWriter) getAvroFieldNamesFromSchema() ([]string, error) {
	fields := p.avroSchema.Schema.Fields()
	avroFieldNames := make([]string, len(fields))
	for i, field := range fields {
		avroFieldNames[i] = field.Name()
	}
	return avroFieldNames, nil
}

func (p *peerDBOCFWriter) writeRecordsToOCFWriter(
	ctx context.Context,
	env map[string]string,
	ocfWriter *ocf.Encoder,
	typeConversions map[string]types.TypeConversion,
	numericTruncator model.SnapshotTableNumericTruncator,
) (int64, error) {
	logger := internal.LoggerFromCtx(ctx)

	avroFieldNames, err := p.getAvroFieldNamesFromSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get Avro field names from schema: %w", err)
	}
	avroConverter, err := model.NewQRecordAvroConverter(
		ctx, env, p.avroSchema, p.targetDWH, avroFieldNames, logger,
	)
	if err != nil {
		return 0, err
	}

	// Create null mismatch tracker if in nullable lax mode
	avroConverter.NullMismatchTracker = model.NewNullMismatchTracker(p.stream.SchemaDebug())

	logger.Info("writing records to OCF start",
		slog.Int("channelLen", len(p.stream.Records)))

	numRows := atomic.Int64{}
	writeStart := time.Now()

	shutdown := shared.Interval(ctx, time.Minute, func() {
		logger.Info("written records to OCF",
			slog.Int64("records", numRows.Load()),
			slog.Int("channelLen", len(p.stream.Records)),
			slog.Float64("elapsedMinutes", time.Since(writeStart).Minutes()),
			slog.String("compression", string(p.avroCompressionCodec)))
	})
	defer shutdown()

	format, err := internal.PeerDBBinaryFormat(ctx, env)
	if err != nil {
		return 0, err
	}

	calcSize := p.sizeTracker != nil && p.sizeTracker.TrackUncompressed
	for qrecord := range p.stream.Records {
		if err := ctx.Err(); err != nil {
			return numRows.Load(), err
		} else {
			avroMap, size, err := avroConverter.Convert(ctx, env, qrecord, typeConversions, numericTruncator, format, calcSize)
			if err != nil {
				logger.Error("Failed to convert QRecord to Avro compatible map", slog.Any("error", err))
				return numRows.Load(), fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
			}

			if err := ocfWriter.Encode(avroMap); err != nil {
				logger.Error("Failed to write record to OCF", slog.Any("error", err))
				return numRows.Load(), fmt.Errorf("failed to write record to OCF: %w", err)
			}

			if calcSize {
				p.sizeTracker.Bytes.Add(size)
			}

			numRows.Add(1)
		}
	}

	logger.Info("finished writing records to OCF",
		slog.Int64("records", numRows.Load()),
		slog.Float64("elapsedMinutes", time.Since(writeStart).Minutes()),
		slog.String("compression", string(p.avroCompressionCodec)),
	)

	if avroConverter.NullMismatchTracker != nil {
		avroConverter.NullMismatchTracker.LogInto(logger)
	}

	if err := p.stream.Err(); err != nil {
		logger.Error("Failed to get record from stream", slog.Any("error", err))
		return numRows.Load(), fmt.Errorf("failed to get record from stream: %w", err)
	}

	return numRows.Load(), nil
}
