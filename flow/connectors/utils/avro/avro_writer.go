package utils

import (
	"context"
	"fmt"
	"io"
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
	log "github.com/sirupsen/logrus"
	uber_atomic "go.uber.org/atomic"
)

type AvroCompressionCodec int64

const (
	CompressNone AvroCompressionCodec = iota
	CompressZstd
	CompressDeflate
	CompressSnappy
)

type peerDBOCFWriter struct {
	ctx                  context.Context
	stream               *model.QRecordStream
	avroSchema           *model.QRecordAvroSchemaDefinition
	avroCompressionCodec AvroCompressionCodec
	writer               io.WriteCloser
	targetDWH            qvalue.QDWHType
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
	schema, err := p.stream.Schema()
	if err != nil {
		log.Errorf("failed to get schema from stream: %v", err)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	colNames := schema.GetColumnNames()

	var numRows uber_atomic.Uint32
	numRows.Store(0)

	if p.ctx != nil {
		shutdown := utils.HeartbeatRoutine(p.ctx, 30*time.Second, func() string {
			written := numRows.Load()
			return fmt.Sprintf("[avro] written %d rows to OCF", written)
		})

		defer func() {
			shutdown <- true
		}()
	}

	for qRecordOrErr := range p.stream.Records {
		if qRecordOrErr.Err != nil {
			log.Errorf("[avro] failed to get record from stream: %v", qRecordOrErr.Err)
			return 0, fmt.Errorf("[avro] failed to get record from stream: %w", qRecordOrErr.Err)
		}

		qRecord := qRecordOrErr.Record
		avroConverter := model.NewQRecordAvroConverter(
			qRecord,
			p.targetDWH,
			p.avroSchema.NullableFields,
			colNames,
		)

		avroMap, err := avroConverter.Convert()
		if err != nil {
			log.Errorf("failed to convert QRecord to Avro compatible map: %v", err)
			return 0, fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			log.Errorf("failed to write record to OCF: %v", err)
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

func (p *peerDBOCFWriter) WriteRecordsToS3(bucketName, key string, s3Creds utils.S3PeerCredentials) (int, error) {
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
		log.Errorf("failed to create S3 client: %v", err)
		return 0, fmt.Errorf("failed to create S3 client: %w", err)
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
		log.Errorf("failed to upload file: %v", err)
		return 0, fmt.Errorf("failed to upload file: %w", err)
	}

	log.Infof("file uploaded to, %s", result.Location)

	return <-numRowsWritten, nil
}

func (p *peerDBOCFWriter) WriteRecordsToAvroFile(filePath string) (int, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	return p.WriteOCF(file)
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
