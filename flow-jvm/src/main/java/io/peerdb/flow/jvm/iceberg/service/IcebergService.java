package io.peerdb.flow.jvm.iceberg.service;

import io.peerdb.flow.jvm.grpc.BranchOptions;
import io.peerdb.flow.jvm.grpc.InsertRecord;
import io.peerdb.flow.jvm.grpc.RecordChange;
import io.peerdb.flow.jvm.grpc.TableInfo;
import io.peerdb.flow.jvm.iceberg.avro.AvroIcebergConverter;
import io.peerdb.flow.jvm.iceberg.catalog.CatalogLoader;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;
import java.util.*;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

@ApplicationScoped
public class IcebergService {

    @Inject
    CatalogLoader catalogLoader;


    public boolean insertChanges(TableInfo tableInfo, String avroSchema, List<RecordChange> recordChanges, Optional<BranchOptions> branchOptions) {
        var icebergCatalog = catalogLoader.loadCatalog(tableInfo.getIcebergCatalog());
        var table = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(tableInfo.getNamespaceList().toArray(String[]::new)), tableInfo.getTableName()));
        if (branchOptions.isPresent()) {
            var branchName = branchOptions.get().getBranch();
            if (table.refs().containsKey(branchName)) {
                switch (branchOptions.get().getBranchCreateConflictPolicy()) {
                    case ERROR ->
                            throw new IllegalArgumentException(String.format("Branch %s already exists", branchName));
                    case IGNORE -> {
                        return false;
                    }
                    case DROP -> table.newTransaction().manageSnapshots().removeBranch(branchName).commit();
                    default -> throw new IllegalArgumentException(String.format("Unrecognized branch create conflict policy %s", branchOptions.get().getBranchCreateConflictPolicy()));
                }
            }
        }
        var appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
        var format = FileFormat.fromString(table.properties().getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
        var outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();
        var targetFileSize = PropertyUtil.propertyAsLong(table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        var converter = new AvroIcebergConverter(avroSchema, table.schema());
        var writer = new UnpartitionedWriter<>(table.spec(), format, appenderFactory, outputFileFactory, table.io(), targetFileSize);
        recordChanges.forEach(recordChange -> {
            switch (recordChange.getChangeCase()) {
                case INSERT:
                    Log.tracef("Inserting record: %s", recordChange.getInsert());
                    var insertRecord = recordChange.getInsert();
                    try {
                        var genericRecord = converter.toIcebergRecord(insertRecord.getRecord().toByteArray());
                    } catch (IOException e) {
                        Log.errorf(e, "Error while converting record");
                        throw new RuntimeException(e);
                    }

                    break;
                case DELETE:
                    Log.tracef("Deleting record: %s", recordChange.getDelete());
                    var deleteRecord = recordChange.getDelete();
                    break;
                case UPDATE:
                    Log.tracef("Updating record: %s", recordChange.getUpdate());
                    var updateRecord = recordChange.getUpdate();
                    break;
            }
//            try {
//                var encoded = Base64.getEncoder().encodeToString(recordChange.getRecord().toByteArray());
//                Log.tracef("Received record: %s", encoded);
//                converter.toIcebergRecord(recordChange.getRecord().toByteArray());
//                var genericRecord = GenericRecord.create(table.schema());
//                Log.tracef("Writing record: %s", genericRecord);
//                writer.write(genericRecord);
//            } catch (IOException e) {
//                Log.errorf(e, "Error while converting/writing record");
//                throw new RuntimeException(e);
//            }
        });



        WriteResult writeResult;
        try {
            writeResult = writer.complete();
        } catch (IOException e) {
            Log.errorf(e, "Error while completing writing records");
            throw new RuntimeException(e);
        }

        var transaction = table.newTransaction();
        branchOptions.ifPresent(options -> transaction.manageSnapshots().createBranch(options.getBranch())
//                .setMaxRefAgeMs()
//                .setMinSnapshotsToKeep()
//                .setMaxSnapshotAgeMs()
                .commit());


        var appendFiles = transaction.newAppend();

        if (branchOptions.isPresent()) {
            appendFiles = appendFiles.toBranch(branchOptions.get().getBranch());
        }

        Arrays.stream(writeResult.dataFiles()).forEach(appendFiles::appendFile);
        appendFiles.commit();
        transaction.commitTransaction();
        return false;
    }

    public Schema getIcebergSchema(String schemaString) {
        var avroSchemaParser = new org.apache.avro.Schema.Parser();
        var avroSchema = avroSchemaParser.parse(schemaString);
        return AvroSchemaUtil.toIceberg(avroSchema);
    }

    public boolean appendRecords(TableInfo tableInfo, String avroSchema, List<InsertRecord> insertRecords) {
        var icebergCatalog = catalogLoader.loadCatalog(tableInfo.getIcebergCatalog());
        var table = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(tableInfo.getNamespaceList().toArray(String[]::new)), tableInfo.getTableName()));

        // TODO add support for identifier fields
        var appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
        var format = FileFormat.fromString(table.properties().getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
        // TODO add partition ID
        var outputFileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();
        var targetFileSize = PropertyUtil.propertyAsLong(table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        var writer = new UnpartitionedWriter<>(table.spec(), format, appenderFactory, outputFileFactory, table.io(), targetFileSize);
        var converter = new AvroIcebergConverter(avroSchema, table.schema());
        // TODO can make this parallel for speed up
        insertRecords.forEach(insertRecord -> {
            try {
                var encoded = Base64.getEncoder().encodeToString(insertRecord.getRecord().toByteArray());
                Log.tracef("Received record: %s", encoded);
                var genericRecord = converter.toIcebergRecord(insertRecord.getRecord().toByteArray());
                Log.tracef("Writing record: %s", genericRecord);
                writer.write(genericRecord);
            } catch (IOException e) {
                Log.errorf(e, "Error while converting/writing record");
                throw new RuntimeException(e);
            }
        });
        WriteResult writeResult;
        try {
            writeResult = writer.complete();
        } catch (IOException e) {
            Log.errorf(e, "Error while completing writing records");
            throw new RuntimeException(e);
        }

        var branchName = "_flow_something_something_temp";

        if (table.refs().containsKey(branchName)) {
            // Maybe skip everything and just return (depending on input) to skip to normalize
            // Or maybe we should drop and redo everything (as there might be some dirty tables)
        }

        var transaction = table.newTransaction();
//        transaction.manageSnapshots().createBranch(branchName)
////                .setMaxRefAgeMs()
////                .setMinSnapshotsToKeep()
////                .setMaxSnapshotAgeMs()
//                .commit();;


        var appendFiles = transaction.newAppend()
//                        .toBranch(branchName)
                ;

        Arrays.stream(writeResult.dataFiles()).forEach(appendFiles::appendFile);
        appendFiles.commit();
        transaction.commitTransaction();

        return true;
    }
}
