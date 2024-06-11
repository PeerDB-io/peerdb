package io.peerdb.flow.jvm.iceberg.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.avro.DataReader;

import java.io.IOException;

public class AvroIcebergConverter {
    private final org.apache.iceberg.Schema icebergSchema;
    private final Schema icebergAvroSchema;
    private final DataReader<org.apache.iceberg.data.GenericRecord> dataReader;

    public AvroIcebergConverter(String avroSchemaString, org.apache.iceberg.Schema icebergSchema, String tableName) {
        this(new Schema.Parser().parse(avroSchemaString), icebergSchema, tableName);

    }

    public AvroIcebergConverter(Schema sourceAvroSchema, org.apache.iceberg.Schema icebergSchema, String tableName) {
        this.icebergSchema = icebergSchema;
        this.icebergAvroSchema = AvroSchemaUtil.convert(icebergSchema, tableName);
        this.dataReader = DataReader.create(icebergSchema, icebergAvroSchema);
        this.dataReader.setSchema(sourceAvroSchema);

    }

    public org.apache.iceberg.data.GenericRecord toIcebergRecord(byte[] avroBytes) throws IOException {
        try (var byteStream = new SeekableByteArrayInput(avroBytes)) {
            var binaryDecoder = DecoderFactory.get().binaryDecoder(byteStream, null);
            return this.dataReader.read(null, binaryDecoder);
        }
    }

    public org.apache.iceberg.Schema getIcebergSchema() {
        return icebergSchema;
    }

    public Schema getIcebergAvroSchema() {
        return icebergAvroSchema;
    }
}
