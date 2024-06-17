package io.peerdb.flow.jvm.iceberg.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;
import io.peerdb.flow.jvm.grpc.*;
import io.peerdb.flow.jvm.iceberg.avro.AvroIcebergRecordConverter;
import io.peerdb.flow.jvm.iceberg.catalog.CatalogLoader;
import io.peerdb.flow.jvm.iceberg.lock.LockManager;
import io.peerdb.flow.jvm.iceberg.writer.RecordWriterFactory;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ApplicationScoped
public class IcebergService {

    static final int maxIdempotencyKeyAgeDays = 7;
    @Inject
    CatalogLoader catalogLoader;
    @Inject
    LockManager lockManager;
    @Inject
    RecordWriterFactory recordWriterFactory;

    private static void writeRecordStream(Stream<InsertRecord> recordStream, AvroIcebergRecordConverter converter, TaskWriter<Record> writer) {
        // use non parallel and use multi everywhere
        recordStream.parallel().map(insertRecord -> {
            try {
                return converter.toIcebergRecord(insertRecord.getRecord().toByteArray());
            } catch (IOException e) {
                Log.errorf(e, "Error while converting record");
                throw new UncheckedIOException(e);
            }
        }).toList().forEach(record -> {
            try {
                writer.write(record);
            } catch (IOException e) {
                Log.errorf(e, "Error while writing record");
                throw new UncheckedIOException(e);
            }
        });
    }

    private Object getTableLockKey(TableInfo tableInfo) {
        return List.of(tableInfo.getIcebergCatalog(), tableInfo.getNamespaceList(), tableInfo.getTableName());
    }

    private TableIdentifier getTableIdentifier(TableInfo tableInfo) {
        return TableIdentifier.parse(tableInfo.getTableName());
    }

    public Table createTable(TableInfo tableInfo, String schema) {
        var icebergCatalog = tableInfo.getIcebergCatalog();

        var catalog = catalogLoader.loadCatalog(icebergCatalog);
        var typeSchema = getIcebergSchema(schema);
        // TODO Below require that the primary keys are non-null
//        var fieldList = typeSchema.columns();
//        var primaryKeyFieldIds = request.getTableInfo().getPrimaryKeyList().stream().map(pk ->
//                Objects.requireNonNull(typeSchema.findField(pk), String.format("Primary key %s not found in schema", pk)).fieldId()
//        ).collect(Collectors.toSet());
//        var icebergSchema = new Schema(fieldList, primaryKeyFieldIds);
        var icebergSchema = typeSchema;
        Preconditions.checkArgument(icebergSchema.asStruct().equals(typeSchema.asStruct()), "Primary key based schema not equivalent to type schema [%s!=%s]", icebergSchema.asStruct(), typeSchema.asStruct());

        var tableIdentifier = getTableIdentifier(tableInfo);
        // We create the namespace if needed
        if (!tableIdentifier.namespace().isEmpty() && catalog instanceof SupportsNamespaces namespacedCatalog && !namespacedCatalog.namespaceExists(tableIdentifier.namespace())) {
            try {
                Log.infof("Creating namespace %s", tableIdentifier.namespace());
                namespacedCatalog.createNamespace(tableIdentifier.namespace());
            } catch (AlreadyExistsException e) {
                Log.warnf("Namespace %s already exists, skipping", tableIdentifier.namespace());
            } catch (UnsupportedOperationException e) {
                Log.warnf("Namespace creation not supported by catalog %s, skipping", icebergCatalog);
            }
        }
        Log.infof("Will now create table %s", tableInfo.getTableName());
        var table = catalog.createTable(tableIdentifier, icebergSchema);
        Log.infof("Created table %s", tableInfo.getTableName());
        return table;
    }

    public boolean dropTable(TableInfo tableInfo, boolean purge) {
        var icebergCatalog = tableInfo.getIcebergCatalog();
        var catalog = catalogLoader.loadCatalog(icebergCatalog);
        return catalog.dropTable(getTableIdentifier(tableInfo), purge);
    }

    public boolean processAppendRecordsRequest(AppendRecordsRequest request) {
        return appendRecords(request.getTableHeader().getTableInfo(),
                request.getTableHeader().getSchema(),
                request.getRecordsList(),
                Optional.ofNullable(request.getTableHeader().hasIdempotencyKey() ? request.getTableHeader().getIdempotencyKey() : null));
    }

    public boolean processAppendRecordsStreamRequest(Multi<AppendRecordsStreamRequest> request) {
        Log.info("Received a Process Append Records Stream Request, will wait for first message");
        // Lets error out after 1 minute
        var firstMessage = Uni.createFrom().multi(request).await().atMost(Duration.ofSeconds(30));
        if (firstMessage.getCommandCase() != AppendRecordsStreamRequest.CommandCase.TABLE_HEADER && !firstMessage.hasTableHeader()) {
            Log.errorf("TableHeader should be present in the first message, found %s", firstMessage.getCommandCase());
            throw new IllegalArgumentException("TableHeader should be present in the first message");
        }
        var tableHeader = firstMessage.getTableHeader();
        var tableInfo = tableHeader.getTableInfo();
        var avroSchema = tableHeader.getSchema();
        Log.infof("Received a Process Append Records Stream Request for table %s, will publish stream for appending records", tableInfo.getTableName());
        var insertRecordStream = request.map(Unchecked.function(message -> {
            if (message.hasRecord()) {
                return message.getRecord();
            } else {
                throw new IllegalArgumentException("Only InsertRecord is supported");
            }
        })).subscribe().asStream();
        return appendRecords(tableInfo, avroSchema, insertRecordStream, Optional.ofNullable(tableHeader.hasIdempotencyKey() ? tableHeader.getIdempotencyKey() : null));
    }

    private boolean appendRecords(TableInfo tableInfo, String avroSchema, List<InsertRecord> insertRecords, Optional<String> idempotencyKey) {
        return appendRecords(tableInfo, avroSchema, insertRecords.stream(), idempotencyKey);
    }

    public boolean appendRecords(TableInfo tableInfo, String avroSchema, Stream<InsertRecord> recordStream, Optional<String> idempotencyKey) {
        var icebergCatalog = catalogLoader.loadCatalog(tableInfo.getIcebergCatalog());
        var table = icebergCatalog.loadTable(getTableIdentifier(tableInfo));

        if (isAppendAlreadyDone(table, idempotencyKey)) {
            return true;
        }

        Log.infof("Converting append records to data files for table %s", table.name());
        var dataFiles = getAppendDataFiles(avroSchema, table, recordStream);
        var recordCount = Arrays.stream(dataFiles).map(ContentFile::recordCount).reduce(0L, Long::sum);
        Log.infof("Converted %d records to %d data files for table %s", recordCount, dataFiles.length, table.name());


        var lockKey = List.of(tableInfo.getIcebergCatalog().toString(), tableInfo.getNamespaceList(), tableInfo.getTableName());
        Log.infof("Will now acquire lock for table %s by idempotency key %s for lockHashCode: %d", table.name(), idempotencyKey.orElse("<not present>"), lockKey.hashCode());
        var lock = lockManager.newLock(lockKey);
        lock.lock();
        try {
            Log.infof("Acquired lock for table %s by idempotency key %s", table.name(), idempotencyKey.orElse("<not present>"));
            Log.infof("Will now refresh table %s", table.name());
            table.refresh();
            if (isAppendAlreadyDone(table, idempotencyKey)) {
                return true;
            }
            var transaction = table.newTransaction();
            Log.infof("Will now append files to table %s", table.name());
            var appendFiles = transaction.newAppend();

            Arrays.stream(dataFiles).forEach(appendFiles::appendFile);
            Log.infof("Appended files to table %s", table.name());
            appendFiles.commit();
            Log.infof("Committed files to table %s", table.name());
            idempotencyKey.ifPresent(key -> {
                Log.infof("Will now create branch %s for table %s", key, table.name());
                transaction.manageSnapshots().createBranch(getBranchNameFromIdempotencyKey(key))
                        .setMaxRefAgeMs(getBranchNameFromIdempotencyKey(key), Duration.ofDays(maxIdempotencyKeyAgeDays).toMillis())
                        .commit();
                Log.infof("Created branch %s for table %s", key, table.name());
            });
            transaction.table().refresh();

            Log.infof("Will now commit transaction for table %s", table.name());
            transaction.commitTransaction();
            Log.infof("Committed transaction for table %s", table.name());

            return true;
        } finally {
            lock.unlock();
            Log.infof("Released lock for table %s by idempotency key %s", table.name(), idempotencyKey.orElse("<not present>"));
        }

    }

    private DataFile[] getAppendDataFiles(String avroSchema, Table table, Stream<InsertRecord> recordStream) {
        WriteResult writeResult;
        try (var writer = recordWriterFactory.createRecordWriter(table)) {
            var converter = new AvroIcebergRecordConverter(avroSchema, table.schema(), table.name());
            Log.infof("Will now write records to append to table %s", table.name());
            var stopwatch = Stopwatch.createStarted();
            writeRecordStream(recordStream, converter, writer);
            Log.infof("Completed writing records to append to table %s in %d ms", table.name(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            try {
                writeResult = writer.complete();
            } catch (IOException e) {
                Log.errorf(e, "Error while completing writing records");
                throw new UncheckedIOException(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return writeResult.dataFiles();
    }

    private boolean isAppendAlreadyDone(Table table, Optional<String> idempotencyKey) {
        if (idempotencyKey.isPresent()) {
            var branchName = getBranchNameFromIdempotencyKey(idempotencyKey.get());
            if (table.refs().containsKey(branchName)) {
                Log.warnf("Already committed work found for table %s with idempotency key %s", table.name(), idempotencyKey.get());
                return true;
            }
        }
        return false;
    }

    public long processTableCountRequest(CountRecordRequest request) {
        var tableInfo = request.getTableInfo();
        var icebergCatalog = tableInfo.getIcebergCatalog();
        var catalog = catalogLoader.loadCatalog(icebergCatalog);
        var table = catalog.loadTable(getTableIdentifier(tableInfo));

        Log.debugf("For table %s, schema is %s", tableInfo.getTableName(), table.schema());
        var count = 0L;
        try (var tableScan = IcebergGenerics.read(table).build()) {
            count = Streams.stream(tableScan.iterator()).reduce(0L, (current, record) -> current + 1L, Long::sum);
        } catch (IOException e) {
            Log.errorf(e, "Error reading table %s", tableInfo.getTableName());
            throw new RuntimeException(e);
        }
        return count;
    }

    public boolean insertChanges(TableInfo tableInfo, String avroSchema, List<RecordChange> recordChanges, Optional<BranchOptions> branchOptions) {
        // TODO this is for CDC, will be done later
        var icebergCatalog = catalogLoader.loadCatalog(tableInfo.getIcebergCatalog());
        var table = icebergCatalog.loadTable(getTableIdentifier(tableInfo));
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
                    default ->
                            throw new IllegalArgumentException(String.format("Unrecognized branch create conflict policy %s", branchOptions.get().getBranchCreateConflictPolicy()));
                }
            }
        }
        var writer = recordWriterFactory.createRecordWriter(table);

        var converter = new AvroIcebergRecordConverter(avroSchema, table.schema(), table.name());
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


    private String getBranchNameFromIdempotencyKey(String idempotencyKey) {
        return String.format("__peerdb-idem-%s", idempotencyKey);
    }
}
