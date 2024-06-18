package io.peerdb.flow.jvm.iceberg.writer;


import io.peerdb.flow.jvm.iceberg.avro.AvroIcebergRecordConverter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

import java.io.IOException;

public class AppendFilesWriter implements AutoCloseable {

    private final TaskWriter<Record> writer;
    private final AvroIcebergRecordConverter converter;

    public AppendFilesWriter(RecordWriterFactory factory, String avroSchema, Table table) {
        this.writer = factory.createRecordWriter(table);
        this.converter = new AvroIcebergRecordConverter(avroSchema, table.schema(), table.name());
    }

    public void writeAvroBytesRecord(byte[] avroBytes) throws IOException {
        var record = converter.toIcebergRecord(avroBytes);
        writer.write(record);
    }


    public DataFile[] complete() throws IOException {
        return writer.complete().dataFiles();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
