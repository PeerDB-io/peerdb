package io.peerdb.flow.jvm.iceberg.resource;

import io.peerdb.flow.jvm.grpc.*;
import io.peerdb.flow.jvm.iceberg.service.IcebergService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.util.Optional;

@GrpcService
public class IcebergResource implements IcebergProxyService {
    @Inject
    IcebergService icebergService;

    @RunOnVirtualThread
    @Override
    public Uni<CreateTableResponse> createTable(CreateTableRequest request) {
        return Uni.createFrom().item(() ->
                CreateTableResponse.newBuilder()
                        .setTableName(
                                icebergService.createTable(request.getTableInfo(), request.getSchema()).name()
                        ).build());
    }


    @RunOnVirtualThread
    @Override
    public Uni<DropTableResponse> dropTable(DropTableRequest request) {
        return Uni.createFrom().item(() -> DropTableResponse.newBuilder().setSuccess(icebergService.dropTable(request.getTableInfo(), request.getPurge())).build());

    }

    @RunOnVirtualThread
    @Override
    public Uni<CountRecordResponse> countRecords(CountRecordRequest request) {
        return Uni.createFrom().item(() -> {
            var count = icebergService.processTableCountRequest(request);
            return CountRecordResponse.newBuilder().setCount(count).build();
        });
    }


    @RunOnVirtualThread
    @Override
    public Uni<InsertChangesResponse> insertChanges(InsertChangesRequest request) {
        return Uni.createFrom()
                .item(() -> InsertChangesResponse.newBuilder()
                        .setSuccess(
                                icebergService.insertChanges(
                                        request.getTableInfo(),
                                        request.getSchema(),
                                        request.getChangesList(),
                                        Optional.ofNullable(request.hasBranchOptions() ? request.getBranchOptions() : null)
                                ))
                        .build());

    }

    @Blocking
    @Override
    public Uni<AppendRecordsResponse> appendRecords(AppendRecordsRequest request) {
        return Uni.createFrom().item(() -> AppendRecordsResponse.newBuilder()
                .setSuccess(
                        icebergService.processAppendRecordsRequest(request))
                .build());
    }
}
