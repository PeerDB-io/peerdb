package utils

import (
	"fmt"
	"io"
	"os"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
)

func createOCFWriter(w io.Writer, avroSchema *model.QRecordAvroSchemaDefinition) (*goavro.OCFWriter, error) {
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      w,
		Schema: avroSchema.Schema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	return ocfWriter, nil
}

func writeRecordsToOCFWriter(
	ocfWriter *goavro.OCFWriter,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
) (int, error) {
	schema, err := stream.Schema()
	if err != nil {
		log.Errorf("failed to get schema from stream: %v", err)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	colNames := schema.GetColumnNames()
	numRows := 0
	for qRecordOrErr := range stream.Records {
		if qRecordOrErr.Err != nil {
			log.Errorf("failed to get record from stream: %v", qRecordOrErr.Err)
			return 0, fmt.Errorf("failed to get record from stream: %w", qRecordOrErr.Err)
		}

		qRecord := qRecordOrErr.Record
		avroConverter := model.NewQRecordAvroConverter(
			qRecord,
			qvalue.QDWHTypeSnowflake,
			&avroSchema.NullableFields,
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

		numRows++
	}

	return numRows, nil
}

func writeOCF(w io.Writer, stream *model.QRecordStream, avroSchema *model.QRecordAvroSchemaDefinition) (int, error) {
	ocfWriter, err := createOCFWriter(w, avroSchema)
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	numRows, err := writeRecordsToOCFWriter(ocfWriter, stream, avroSchema)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to OCF writer: %w", err)
	}

	return numRows, nil
}

func WriteRecordsToS3(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	bucketName, key string) (int, error) {
	r, w := io.Pipe()

	numRowsWritten := make(chan int, 1)
	go func() {
		defer w.Close()

		numRows, err := writeOCF(w, stream, avroSchema)
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

func WriteRecordsToAvroFile(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	filePath string,
) (int, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return writeOCF(file, stream, avroSchema)
}
