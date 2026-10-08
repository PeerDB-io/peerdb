package utils

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal/benchfixtures"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

// BenchmarkRecordStreamToS3 measures the production CDC conversion, Avro, Zstd,
// pipe and uploader. Fixture construction and channel prefill are not timed.
// With PEERDB_BENCH_S3=1 the S3 case uploads to the configured local MinIO.
// The discard case isolates serialization from network/storage variability.
func BenchmarkRecordStreamToS3(b *testing.B) {
	testutil.LoadEnv()
	const rows = 32768
	env := map[string]string{
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
		"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		"PEERDB_S3_PART_SIZE":                           "8388608",
	}
	fixtures := make([]model.Record[model.RecordItems], rows)
	for i := range fixtures {
		fixtures[i] = &model.InsertRecord[model.RecordItems]{Items: benchfixtures.Record(i), DestinationTableName: "bench"}
	}
	for _, upload := range []bool{false, true} {
		b.Run(fmt.Sprintf("s3=%t", upload), func(b *testing.B) {
			if upload && os.Getenv("PEERDB_BENCH_S3") != "1" {
				b.Skip("set PEERDB_BENCH_S3=1 for local MinIO upload")
			}
			ctx := b.Context()
			var provider AWSCredentialsProvider
			var client *s3.Client
			key := fmt.Sprintf("serialization-bench/%d.avro", os.Getpid())
			if upload {
				var err error
				provider, err = GetAWSCredentialsProvider(ctx, "bench", NewPeerAWSCredentials(&protos.S3Config{
					AccessKeyId: new(os.Getenv("AWS_ACCESS_KEY_ID")), SecretAccessKey: new(os.Getenv("AWS_SECRET_ACCESS_KEY")),
					Region: new(os.Getenv("AWS_REGION")), Endpoint: new(os.Getenv("AWS_ENDPOINT_URL_S3")),
				}))
				if err != nil {
					b.Fatal(err)
				}
				client, err = CreateS3Client(ctx, provider)
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: new("peerdb"), Key: &key}); err != nil {
						b.Error(err)
					}
				}()
			}
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				records := make(chan model.Record[model.RecordItems], rows)
				for _, record := range fixtures {
					records <- record
				}
				close(records)
				counts := map[string]*model.RecordTypeCounts{"bench": {}}
				req := model.NewRecordsToStreamRequest(records, counts, 1, false, protos.DBType_CLICKHOUSE)
				// Match production's lazy per-column numeric tracking, including its cost.
				truncator := model.NewStreamNumericTruncator([]*protos.TableMapping{{DestinationTableIdentifier: "bench"}}, nil)
				b.StartTimer()
				stream, err := RecordsToRawTableStream(req, truncator)
				if err != nil {
					b.Fatal(err)
				}
				schema, err := stream.Schema()
				if err != nil {
					b.Fatal(err)
				}
				avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, "bench", schema, protos.DBType_CLICKHOUSE, nil)
				if err != nil {
					b.Fatal(err)
				}
				writer := NewPeerDBOCFWriter(stream, avroSchema, ocf.ZStandard, protos.DBType_CLICKHOUSE, nil)
				var n int64
				if upload {
					file, e := writer.WriteRecordsToS3(ctx, env, "peerdb", key, provider, nil, nil)
					err = e
					n = file.NumRecords
				} else {
					n, err = writer.WriteOCF(ctx, env, io.Discard, nil, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
				if n != rows || counts["bench"].InsertCount.Load() != rows {
					b.Fatalf("lost rows: %d", n)
				}
			}
			b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			if upload {
				object, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new("peerdb"), Key: &key})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(*object.ContentLength)/rows, "compressed-B/row")
			}
		})
	}
}
