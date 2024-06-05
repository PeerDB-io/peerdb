package io.peerdb.flow.jvm.iceberg.resource;

import com.google.common.collect.Streams;
import io.peerdb.flow.jvm.grpc.*;
import io.peerdb.flow.jvm.iceberg.catalog.CatalogLoader;
import io.peerdb.flow.jvm.iceberg.service.IcebergService;
import io.quarkus.grpc.GrpcService;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;

import java.io.IOException;
import java.util.Optional;

@GrpcService
public class IcebergResource implements IcebergProxyService {
    @Inject
    CatalogLoader catalogLoader;

    @Inject
    IcebergService icebergService;

    @RunOnVirtualThread
    @Override
    public Uni<CreateTableResponse> createTable(CreateTableRequest request) {
        var tableInfo = request.getTableInfo();
        var icebergCatalog = tableInfo.getIcebergCatalog();

        var catalog = catalogLoader.loadCatalog(icebergCatalog);
        var icebergSchema = icebergService.getIcebergSchema(request.getSchema());
        var table = catalog.createTable(TableIdentifier.of(Namespace.of(tableInfo.getNamespaceList().toArray(String[]::new)), tableInfo.getTableName()), icebergSchema);
        return Uni.createFrom().item(CreateTableResponse.newBuilder().setTableName(table.name()).build());
    }


    @RunOnVirtualThread
    @Override
    public Uni<DropTableResponse> dropTable(DropTableRequest request) {
        var tableInfo = request.getTableInfo();
        var icebergCatalog = tableInfo.getIcebergCatalog();
        var catalog = catalogLoader.loadCatalog(icebergCatalog);
        var droppedTable = catalog.dropTable(TableIdentifier.of(Namespace.of(tableInfo.getNamespaceList().toArray(String[]::new)), tableInfo.getTableName()), request.getPurge());
        return Uni.createFrom().item(DropTableResponse.newBuilder().setSuccess(droppedTable).build());

    }

    @Override
    public Uni<CountRecordResponse> countRecords(CountRecordRequest request) {
        var tableInfo = request.getTableInfo();
        var icebergCatalog = tableInfo.getIcebergCatalog();
        var catalog = catalogLoader.loadCatalog(icebergCatalog);
        var table = catalog.loadTable(TableIdentifier.of(Namespace.of(tableInfo.getNamespaceList().toArray(String[]::new)), tableInfo.getTableName()));

        Log.debugf("For table %s, schema is %s", tableInfo.getTableName(), table.schema());
        var count = 0L;
        try (var tableScan = IcebergGenerics.read(table).build()) {
            count = Streams.stream(tableScan.iterator()).reduce(0L, (current, record) -> current + 1L, Long::sum);
        } catch (IOException e) {
            Log.errorf(e, "Error reading table %s", tableInfo.getTableName());
            throw new RuntimeException(e);
        }
        return Uni.createFrom().item(CountRecordResponse.newBuilder().setCount(count).build());
    }

    @RunOnVirtualThread
    @Override
    public Uni<InsertChangesResponse> insertChanges(InsertChangesRequest request) {
        return Uni.createFrom()
                .item(InsertChangesResponse.newBuilder()
                        .setSuccess(
                                icebergService.insertChanges(
                                        request.getTableInfo(),
                                        request.getSchema(),
                                        request.getChangesList(),
                                        Optional.ofNullable(request.hasBranchOptions() ? request.getBranchOptions() : null)
                                ))
                        .build());

    }

    @Override
    public Uni<AppendRecordsResponse> appendRecords(AppendRecordsRequest request) {
        return Uni.createFrom().item(AppendRecordsResponse.newBuilder().setSuccess(icebergService.appendRecords(request.getTableInfo(), request.getSchema(), request.getRecordsList())).build());
    }
}
