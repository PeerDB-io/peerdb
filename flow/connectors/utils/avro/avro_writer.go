package utils

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"go.temporal.io/sdk/activity"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
)

type PeerDBOCFWriter struct {
	ctx        context.Context
	stream     *model.QRecordStream
	avroSchema *model.QRecordAvroSchemaDefinition
}

func NewPeerDBOCFWriter(
	ctx context.Context,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
) *PeerDBOCFWriter {
	return &PeerDBOCFWriter{
		ctx:        ctx,
		stream:     stream,
		avroSchema: avroSchema,
	}
}

func (p *PeerDBOCFWriter) createOCFWriter(w io.Writer) (*goavro.OCFWriter, error) {
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      w,
		Schema: p.avroSchema.Schema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF writer: %w", err)
	}
	return ocfWriter, nil
}

func (p *PeerDBOCFWriter) writeRecordsToOCFWriter(ocfWriter *goavro.OCFWriter) (int, error) {
	schema, err := p.stream.Schema()
	if err != nil {
		log.Errorf("failed to get schema from stream: %v", err)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	colNames := schema.GetColumnNames()
	numRows := 0
	const heartBeatNumRows = 1000
	for qRecordOrErr := range p.stream.Records {
		if qRecordOrErr.Err != nil {
			log.Errorf("[avro] failed to get record from stream: %v", qRecordOrErr.Err)
			return 0, fmt.Errorf("[avro] failed to get record from stream: %w", qRecordOrErr.Err)
		}

		qRecord := qRecordOrErr.Record
		avroConverter := model.NewQRecordAvroConverter(
			qRecord,
			qvalue.QDWHTypeSnowflake,
			&p.avroSchema.NullableFields,
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

		if numRows%heartBeatNumRows == 0 {
			log.Infof("written %d rows to OCF", numRows)
			msg := fmt.Sprintf("written %d rows to OCF", numRows)
			if p.ctx != nil {
				activity.RecordHeartbeat(p.ctx, msg)
			}
		}

		numRows++
	}

	if p.ctx != nil {
		msg := fmt.Sprintf("written all: %d rows to OCF", numRows)
		activity.RecordHeartbeat(p.ctx, msg)
	}

	return numRows, nil
}

func (p *PeerDBOCFWriter) WriteOCF(w io.Writer) (int, error) {
	ocfWriter, err := p.createOCFWriter(w)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}
	numRows, err := p.writeRecordsToOCFWriter(ocfWriter)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to OCF writer: %w", err)
	}
	return numRows, nil
}

func (p *PeerDBOCFWriter) WriteRecordsToS3(bucketName, key string) (int, error) {
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

	s3svc, err := utils.CreateS3Client()
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

func (p *PeerDBOCFWriter) WriteRecordsToAvroFile(filePath string) (int, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	return p.WriteOCF(file)
}
