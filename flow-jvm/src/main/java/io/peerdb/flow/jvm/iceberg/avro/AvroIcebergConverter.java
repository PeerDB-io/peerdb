package io.peerdb.flow.jvm.iceberg.avro;

import io.quarkus.logging.Log;
import org.apache.avro.Schema;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.data.Record;

import java.io.IOException;

public class AvroIcebergConverter {
    private final org.apache.iceberg.Schema icebergSchema;
    private final Schema avroSchema;

    public AvroIcebergConverter(String avroSchemaString, org.apache.iceberg.Schema icebergSchema) {

        this.icebergSchema = icebergSchema;
        var avroSchemaParser = new org.apache.avro.Schema.Parser();
        this.avroSchema = avroSchemaParser.parse(avroSchemaString);
    }

    public AvroIcebergConverter(Schema avroSchema, org.apache.iceberg.Schema icebergSchema) {
        this.icebergSchema = icebergSchema;
        this.avroSchema = avroSchema;
    }

    public GenericRecord toAvroRecord(byte[] bytes) throws IOException {
        var reader = new GenericDatumReader<GenericRecord>(avroSchema);
        try (var byteStream = new SeekableByteArrayInput(bytes)) {
        // The below code is for avro binary data files
//                    var dataFileReader = DataFileReader.openReader(byteStream, reader);
//                    if (!dataFileReader.hasNext()) {
//                        Log.errorf("No records found!");
//                        return;
//                    }
//                    var record = dataFileReader.next();

            var binaryDecoder = DecoderFactory.get().binaryDecoder(byteStream, null);
            return reader.read(null, binaryDecoder);
        }
    }


    public org.apache.iceberg.data.GenericRecord toIcebergRecord(GenericRecord avroRecord) {
        var genericRecord = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
        for (var field : icebergSchema.columns()) {
            var fieldName = field.name();
            var fieldValue = avroRecord.get(fieldName);
            Log.tracef("Will set Field: %s, Value: %s, Current Record: %s", fieldName, fieldValue, genericRecord);
            genericRecord.setField(fieldName, fieldValue);
        }
        return genericRecord;
    }

    public org.apache.iceberg.data.GenericRecord toIcebergRecord(byte[] bytes) throws IOException {
        return toIcebergRecord(toAvroRecord(bytes));
    }

}
