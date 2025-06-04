package utils

import (
	"bufio"
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
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
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
	avroCompressionCodec ocf.CodecName
	targetDWH            protos.DBType
}

type AvroFile struct {
	FilePath        string              `json:"filePath"`
	StorageLocation AvroStorageLocation `json:"storageLocation"`
	NumRecords      int64               `json:"numRecords"`
}

func (l *AvroFile) Cleanup() {
	if l.StorageLocation == AvroLocalStorage {
		err := os.Remove(l.FilePath)
		if err != nil && !os.IsNotExist(err) {
			slog.Warn("unable to delete temporary Avro file", slog.Any("error", err))
		}
	}
}

func NewPeerDBOCFWriter(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	avroCompressionCodec ocf.CodecName,
	targetDWH protos.DBType,
) *peerDBOCFWriter {
	return &peerDBOCFWriter{
		stream:               stream,
		avroSchema:           avroSchema,
		avroCompressionCodec: avroCompressionCodec,
		targetDWH:            targetDWH,
	}
}

func (p *peerDBOCFWriter) WriteOCF(
	ctx context.Context,
	env map[string]string,
	w io.Writer,
	typeConversions map[string]qvalue.TypeConversion,
) (int64, error) {
	ocfWriter, err := p.createOCFWriter(w)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}
	defer ocfWriter.Close()

	numRows, err := p.writeRecordsToOCFWriter(ctx, env, ocfWriter, typeConversions)
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
	s3Creds utils.AWSCredentialsProvider,
	avroSize *atomic.Int64,
	typeConversions map[string]qvalue.TypeConversion,
) (*AvroFile, error) {
	logger := internal.LoggerFromCtx(ctx)
	s3svc, err := utils.CreateS3Client(ctx, s3Creds)
	if err != nil {
		logger.Error("failed to create S3 client", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	buf := buffer.New(32 * 1024 * 1024) // 32MB in memory Buffer
	r, w := nio.Pipe(buf)

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
		if avroSize == nil {
			writer = w
		} else {
			writer = shared.NewWatchWriter(w, avroSize)
		}
		numRows, writeOcfError = p.WriteOCF(ctx, env, writer, typeConversions)
	}()

	partSize, err := internal.PeerDBS3PartSize(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("could not get s3 part size config: %w", err)
	}

	// Create the uploader using the AWS SDK v2 manager
	uploader := manager.NewUploader(s3svc, func(u *manager.Uploader) {
		if partSize > 0 {
			u.PartSize = partSize
		}
	})

	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   r,
	}); err != nil {
		s3Path := "s3://" + bucketName + "/" + key
		logger.Error("failed to upload file", slog.Any("error", err), slog.String("s3_path", s3Path))
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}

	if writeOcfError != nil {
		logger.Error("failed to write records to OCF", slog.Any("error", writeOcfError))
		return nil, writeOcfError
	}

	return &AvroFile{
		StorageLocation: AvroS3Storage,
		FilePath:        key,
		NumRecords:      numRows,
	}, nil
}

func (p *peerDBOCFWriter) WriteRecordsToAvroFile(ctx context.Context, env map[string]string, filePath string) (*AvroFile, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary Avro file: %w", err)
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

	buffSizeBytes := 1 << 26 // 64 MB
	bufferedWriter := bufio.NewWriterSize(file, buffSizeBytes)
	defer bufferedWriter.Flush()

	numRecords, err := p.WriteOCF(ctx, env, bufferedWriter, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to write records to temporary Avro file: %w", err)
	}

	printFileStats("finished writing to temporary Avro file")
	return &AvroFile{
		NumRecords:      numRecords,
		StorageLocation: AvroLocalStorage,
		FilePath:        filePath,
	}, nil
}

func (p *peerDBOCFWriter) createOCFWriter(w io.Writer) (*ocf.Encoder, error) {
	ocfWriter, err := ocf.NewEncoderWithSchema(
		p.avroSchema.Schema, w, ocf.WithCodec(p.avroCompressionCodec),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	return ocfWriter, nil
}

func (p *peerDBOCFWriter) getAvroFieldNamesFromSchemaJSON() ([]string, error) {
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
	typeConversions map[string]qvalue.TypeConversion,
) (int64, error) {
	logger := internal.LoggerFromCtx(ctx)

	avroFieldNames, err := p.getAvroFieldNamesFromSchemaJSON()
	if err != nil {
		return 0, fmt.Errorf("failed to get Avro field names from schema JSON: %w", err)
	}
	avroConverter, err := model.NewQRecordAvroConverter(
		ctx, env, p.avroSchema, p.targetDWH, avroFieldNames, logger,
	)
	if err != nil {
		return 0, err
	}

	numRows := atomic.Int64{}

	shutdown := shared.Interval(ctx, time.Minute, func() {
		logger.Info(fmt.Sprintf("written %d records to OCF", numRows.Load()))
	})
	defer shutdown()

	for qrecord := range p.stream.Records {
		if err := ctx.Err(); err != nil {
			return numRows.Load(), err
		} else {
			avroMap, err := avroConverter.Convert(ctx, env, qrecord, typeConversions)
			if err != nil {
				logger.Error("Failed to convert QRecord to Avro compatible map", slog.Any("error", err))
				return numRows.Load(), fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
			}

			if err := ocfWriter.Encode(avroMap); err != nil {
				logger.Error("Failed to write record to OCF", slog.Any("error", err))
				return numRows.Load(), fmt.Errorf("failed to write record to OCF: %w", err)
			}

			numRows.Add(1)
		}
	}

	if err := p.stream.Err(); err != nil {
		logger.Error("Failed to get record from stream", slog.Any("error", err))
		return numRows.Load(), fmt.Errorf("failed to get record from stream: %w", err)
	}

	return numRows.Load(), nil
}
