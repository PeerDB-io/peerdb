package io.peerdb.flow.jvm.iceberg.writer;


import jakarta.enterprise.context.ApplicationScoped;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;

import java.util.UUID;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

@ApplicationScoped
public class RecordWriterFactory {
    public TaskWriter<Record> createRecordWriter(Table table) {
        // TODO add support for partition ID
        return createUnpartitionedRecordWriter(table);
    }


    private TaskWriter<Record> createUnpartitionedRecordWriter(Table table) {
        var appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
        var format = FileFormat.fromString(table.properties().getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
        var outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();
        var targetFileSize = PropertyUtil.propertyAsLong(table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        return new UnpartitionedWriter<>(table.spec(), format, appenderFactory, outputFileFactory, table.io(), targetFileSize);
    }

}
