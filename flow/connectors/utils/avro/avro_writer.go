package utils

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/linkedin/goavro/v2"
	uber_atomic "go.uber.org/atomic"
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
	ctx                  context.Context
	stream               *model.QRecordStream
	avroSchema           *model.QRecordAvroSchemaDefinition
	avroCompressionCodec AvroCompressionCodec
	writer               io.WriteCloser
	targetDWH            qvalue.QDWHType
}

type AvroFile struct {
	NumRecords      int
	StorageLocation AvroStorageLocation
	FilePath        string
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
	ctx context.Context,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	avroCompressionCodec AvroCompressionCodec,
	targetDWH qvalue.QDWHType,
) *peerDBOCFWriter {
	return &peerDBOCFWriter{
		ctx:                  ctx,
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

func (p *peerDBOCFWriter) writeRecordsToOCFWriter(ocfWriter *goavro.OCFWriter) (int, error) {
	fmt.Printf("\n**************************************** writeRecordsToOCFWriter 1")
	schema, err := p.stream.Schema()
	if err != nil {
		slog.Error("failed to get schema from stream", slog.Any("error", err))
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}
	fmt.Printf("\n**************************************** writeRecordsToOCFWriter 2 schema: %+v", schema)

	colNames := schema.GetColumnNames()
	fmt.Printf("\n**************************************** writeRecordsToOCFWriter 3 colNames: %+v", colNames)

	var numRows uber_atomic.Uint32

	numRows.Store(0)

	if p.ctx != nil {
		shutdown := utils.HeartbeatRoutine(p.ctx, 30*time.Second, func() string {
			written := numRows.Load()
			return fmt.Sprintf("[avro] written %d rows to OCF", written)
		})

		defer func() {
			shutdown <- struct{}{}
		}()
	}

	for qRecordOrErr := range p.stream.Records {
		if qRecordOrErr.Err != nil {
			slog.Error("[avro] failed to get record from stream", slog.Any("error", qRecordOrErr.Err))
			return 0, fmt.Errorf("[avro] failed to get record from stream: %w", qRecordOrErr.Err)
		}
		avroConverter := model.NewQRecordAvroConverter(
			qRecordOrErr.Record,
			p.targetDWH,
			p.avroSchema.NullableFields,
			colNames,
		)

		avroMap, err := avroConverter.Convert()

		if err != nil {
			slog.Error("failed to convert QRecord to Avro compatible map: ", slog.Any("error", err))
			return 0, fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		fmt.Printf("\n *********************** in writeRecordsToOCFWriter avroMap %+v  \n", avroMap)

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			slog.Error("failed to write record to OCF: ", slog.Any("error", err))
			return 0, fmt.Errorf("failed to write record to OCF: %w", err)
		}

		numRows.Inc()
	}

	return int(numRows.Load()), nil
}

func (p *peerDBOCFWriter) WriteOCF(w io.Writer) (int, error) {
	ocfWriter, err := p.createOCFWriter(w)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}
	// we have to keep a reference to the underlying writer as goavro doesn't provide any access to it
	defer p.writer.Close()

	numRows, err := p.writeRecordsToOCFWriter(ocfWriter)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to OCF writer: %w", err)
	}
	return numRows, nil
}

func (p *peerDBOCFWriter) WriteRecordsToS3(bucketName, key string, s3Creds utils.S3PeerCredentials) (*AvroFile, error) {
	r, w := io.Pipe()
	numRowsWritten := make(chan int, 1)
	go func() {
		defer w.Close()
		numRows, err := p.WriteOCF(w)
		if err != nil {
			log.Fatalf("%v", err)
		}
		numRowsWritten <- numRows
	}()

	s3svc, err := utils.CreateS3Client(s3Creds)
	if err != nil {
		slog.Error("failed to create S3 client: ", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploaderWithClient(s3svc)

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		slog.Error("failed to upload file: ", slog.Any("error", err))
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}

	slog.Info("file uploaded to" + result.Location)

	return &AvroFile{
		NumRecords:      <-numRowsWritten,
		StorageLocation: AvroS3Storage,
		FilePath:        key,
	}, nil
}

func (p *peerDBOCFWriter) WriteRecordsToAvroFile(filePath string) (*AvroFile, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary Avro file: %w", err)
	}

	numRecords, err := p.WriteOCF(file)
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
