package io.peerdb.flow.jvm.iceberg.resource;

import io.peerdb.flow.jvm.grpc.AppendRecordsRequest;
import io.peerdb.flow.jvm.grpc.AppendRecordsResponse;
import io.peerdb.flow.jvm.grpc.AppendRecordsStreamRequest;
import io.peerdb.flow.jvm.grpc.AppendRecordsStreamResponse;
import io.peerdb.flow.jvm.grpc.CountRecordRequest;
import io.peerdb.flow.jvm.grpc.CountRecordResponse;
import io.peerdb.flow.jvm.grpc.CreateTableRequest;
import io.peerdb.flow.jvm.grpc.CreateTableResponse;
import io.peerdb.flow.jvm.grpc.DropTableRequest;
import io.peerdb.flow.jvm.grpc.DropTableResponse;
import io.peerdb.flow.jvm.grpc.IcebergProxyService;
import io.peerdb.flow.jvm.grpc.InsertChangesRequest;
import io.peerdb.flow.jvm.grpc.InsertChangesResponse;
import io.peerdb.flow.jvm.iceberg.service.IcebergService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Multi;
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

    /**
     * Append records to the iceberg table with records all encoded in the request body
     * @deprecated Use {@link #streamingAppendRecords(Multi)} instead for better performance
     * @param request AppendRecordsRequest containing the records to be appended along with table info
     * @return AppendRecordsResponse containing the success status of the operation
     */
    @Blocking
    @Override
    @Deprecated
    public Uni<AppendRecordsResponse> appendRecords(AppendRecordsRequest request) {
        return Uni.createFrom().item(() -> AppendRecordsResponse.newBuilder()
                .setSuccess(
                        icebergService.processAppendRecordsRequest(request))
                .build());
    }

    /**
     * Append records to the iceberg table with records streamed in the request body
     * @param request AppendRecordsStreamRequest containing the records to be appended along with table info as the first record
     * @return AppendRecordsStreamResponse containing the success status of the operation
     */
    @Blocking
    @Override
    public Uni<AppendRecordsStreamResponse> streamingAppendRecords(Multi<AppendRecordsStreamRequest> request) {
        return icebergService.appendRecordsAsync(request);
    }
}
