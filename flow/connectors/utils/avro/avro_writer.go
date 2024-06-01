package utils

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/linkedin/goavro/v2"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
)

type (
	AvroCompressionCodec int64
	AvroStorageLocation  int64
)

const (
	CompressNone AvroCompressionCodec = iota
	CompressZstd
	CompressDeflate
	CompressSnappy
)

const (
	AvroLocalStorage = iota
	AvroS3Storage
	AvroGCSStorage
)

type peerDBOCFWriter struct {
	stream               *model.QRecordStream
	avroSchema           *model.QRecordAvroSchemaDefinition
	writer               io.WriteCloser
	avroCompressionCodec AvroCompressionCodec
	targetDWH            protos.DBType
}

type AvroFile struct {
	FilePath        string
	StorageLocation AvroStorageLocation
	NumRecords      int
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
	avroCompressionCodec AvroCompressionCodec,
	targetDWH protos.DBType,
) *peerDBOCFWriter {
	return &peerDBOCFWriter{
		stream:               stream,
		avroSchema:           avroSchema,
		avroCompressionCodec: avroCompressionCodec,
		targetDWH:            targetDWH,
	}
}

func (p *peerDBOCFWriter) initWriteCloser(w io.Writer) error {
	var err error
	switch p.avroCompressionCodec {
	case CompressNone:
		p.writer = &nopWriteCloser{w}
	case CompressZstd:
		p.writer, err = zstd.NewWriter(w)
		if err != nil {
			return fmt.Errorf("error while initializing zstd encoding writer: %w", err)
		}
	case CompressDeflate:
		p.writer, err = flate.NewWriter(w, -1)
		if err != nil {
			return fmt.Errorf("error while initializing deflate encoding writer: %w", err)
		}
	case CompressSnappy:
		p.writer = snappy.NewBufferedWriter(w)
	}

	return nil
}

func (p *peerDBOCFWriter) createOCFWriter(w io.Writer) (*goavro.OCFWriter, error) {
	err := p.initWriteCloser(w)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressed writer: %w", err)
	}

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      p.writer,
		Schema: p.avroSchema.Schema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	return ocfWriter, nil
}

func (p *peerDBOCFWriter) writeRecordsToOCFWriter(ctx context.Context, ocfWriter *goavro.OCFWriter, maxRecords int) (int, error) {
	logger := logger.LoggerFromCtx(ctx)
	schema := p.stream.Schema()

	avroConverter := model.NewQRecordAvroConverter(
		p.avroSchema,
		p.targetDWH,
		schema.GetColumnNames(),
		logger,
	)

	numRows := 0
	for qrecord := range p.stream.Records {
		avroMap, err := avroConverter.Convert(qrecord)
		if err != nil {
			logger.Error("failed to convert QRecord to Avro compatible map: ", slog.Any("error", err))
			return 0, fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			logger.Error("failed to write record to OCF: ", slog.Any("error", err))
			return 0, fmt.Errorf("failed to write record to OCF: %w", err)
		}

		numRows += 1
		if maxRecords > 0 && numRows >= maxRecords {
			break
		}
	}
	if err := p.stream.Err(); err != nil {
		logger.Error("[avro] failed to get record from stream", slog.Any("error", err))
		return 0, fmt.Errorf("[avro] failed to get record from stream: %w", err)
	}

	return numRows, nil
}

func (p *peerDBOCFWriter) WriteOCF(ctx context.Context, w io.Writer, maxRecords int) (int, error) {
	ocfWriter, err := p.createOCFWriter(w)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}
	// we have to keep a reference to the underlying writer as goavro doesn't provide any access to it
	defer p.writer.Close()

	numRows, err := p.writeRecordsToOCFWriter(ctx, ocfWriter, maxRecords)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to OCF writer: %w", err)
	}
	return numRows, nil
}

func (p *peerDBOCFWriter) WriteRecordsToS3Parts(
	ctx context.Context, bucketName, keyPrefix string, s3Creds utils.AWSCredentialsProvider, maxRecordsPerFile int,
) ([]*AvroFile, error) {
	logger := logger.LoggerFromCtx(ctx)
	s3svc, err := utils.CreateS3Client(ctx, s3Creds)
	if err != nil {
		logger.Error("failed to create S3 client: ", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	var avroFiles []*AvroFile
	part := 0
	for {
		buf := buffer.New(32 * 1024 * 1024) // 32MB in memory Buffer
		r, w := nio.Pipe(buf)

		var writeOcfError error
		var numRows int

		go func() {
			defer func() {
				if r := recover(); r != nil {
					writeOcfError = fmt.Errorf("panic occurred during WriteOCF: %v", r)
					stack := string(debug.Stack())
					logger.Error("panic during WriteOCF", slog.Any("error", writeOcfError), slog.String("stack", stack))
				}
				w.Close()
			}()
			numRows, writeOcfError = p.WriteOCF(ctx, w, maxRecordsPerFile)
		}()

		key := fmt.Sprintf("%s_part_%d.avro.zst", keyPrefix, part)
		_, err = manager.NewUploader(s3svc).Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   r,
		})
		if err != nil {
			s3Path := "s3://" + bucketName + "/" + key
			logger.Error("failed to upload file: ", slog.Any("error", err), slog.Any("s3_path", s3Path))
			return nil, fmt.Errorf("failed to upload file to path %s: %w", s3Path, err)
		}

		if writeOcfError != nil {
			logger.Error("failed to write records to OCF: ", slog.Any("error", writeOcfError))
			return nil, writeOcfError
		}

		avroFiles = append(avroFiles, &AvroFile{
			NumRecords:      numRows,
			StorageLocation: AvroS3Storage,
			FilePath:        key,
		})

		if numRows < maxRecordsPerFile {
			break
		}
		part++
	}

	return avroFiles, nil
}

func (p *peerDBOCFWriter) WriteRecordsToS3(
	ctx context.Context, bucketName, key string, s3Creds utils.AWSCredentialsProvider,
) (*AvroFile, error) {
	avroFiles, err := p.WriteRecordsToS3Parts(ctx, bucketName, key, s3Creds, -1)
	if err != nil {
		return nil, err
	}
	if len(avroFiles) != 1 {
		return nil, fmt.Errorf("unexpected number of Avro files: expected 1, got %d", len(avroFiles))
	}
	return avroFiles[0], nil
}

func (p *peerDBOCFWriter) WriteRecordsToAvroFile(ctx context.Context, filePath string) (*AvroFile, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary Avro file: %w", err)
	}

	numRecords, err := p.WriteOCF(ctx, file, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to write records to temporary Avro file: %w", err)
	}

	return &AvroFile{
		NumRecords:      numRecords,
		StorageLocation: AvroLocalStorage,
		FilePath:        filePath,
	}, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (n *nopWriteCloser) Close() error {
	if closer, ok := n.Writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
