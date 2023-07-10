package conns3

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
	records *model.QRecordBatch,
	avroSchema *model.QRecordAvroSchemaDefinition) error {
	colNames := records.Schema.GetColumnNames()

	for _, qRecord := range records.Records {
		avroConverter := model.NewQRecordAvroConverter(
			qRecord,
			qvalue.QDWHTypeSnowflake,
			&avroSchema.NullableFields,
			colNames,
		)
		avroMap, err := avroConverter.Convert()
		if err != nil {
			log.Errorf("failed to convert QRecord to Avro compatible map: %v", err)
			return fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			log.Errorf("failed to write record to OCF: %v", err)
			return fmt.Errorf("failed to write record to OCF: %w", err)
		}
	}

	return nil
}

func WriteRecordsToS3(
	records *model.QRecordBatch,
	avroSchema *model.QRecordAvroSchemaDefinition,
	bucketName, key string) error {
	r, w := io.Pipe()

	go func() {
		defer w.Close()

		ocfWriter, err := createOCFWriter(w, avroSchema)
		if err != nil {
			log.Fatalf("failed to create OCF writer: %v", err)
		}

		if err := writeRecordsToOCFWriter(ocfWriter, records, avroSchema); err != nil {
			log.Fatalf("failed to write records to OCF writer: %v", err)
		}
	}()

	s3svc, err := utils.CreateS3Client()
	if err != nil {
		log.Errorf("failed to create S3 client: %v", err)
		return fmt.Errorf("failed to create S3 client: %w", err)
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
		return fmt.Errorf("failed to upload file: %w", err)
	}

	log.Infof("file uploaded to, %s", result.Location)

	return nil
}

func WriteRecordsToAvroFile(
	records *model.QRecordBatch,
	avroSchema *model.QRecordAvroSchemaDefinition,
	filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		log.Errorf("failed to create file: %v", err)
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	ocfWriter, err := createOCFWriter(file, avroSchema)
	if err != nil {
		log.Errorf("failed to create OCF writer: %v", err)
		return err
	}

	if err := writeRecordsToOCFWriter(ocfWriter, records, avroSchema); err != nil {
		log.Errorf("failed to write records to OCF writer: %v", err)
		return err
	}

	return nil
}
