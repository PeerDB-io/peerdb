//package io.peerdb;
//
//import io.peerdb.flow.jvm.iceberg.resource.IcebergResource;
//import io.peerdb.flow.jvm.iceberg.service.IcebergService;
//import io.quarkus.grpc.GrpcClient;
//import io.quarkus.test.junit.QuarkusTest;
//import org.junit.jupiter.api.Test;
//
//@QuarkusTest
//public class IcebergServiceTest {
//    @GrpcClient
//    IcebergService client;
//
//    @Test
//    public void printRecordCount() {
//        var response = client.createTable(null).onFailure().invoke(e -> {
//            System.out.println(e);
//        }).onFailure().recoverWithNull().await().indefinitely();
//        System.out.println(response);
//    }
//
//}
