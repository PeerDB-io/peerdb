package io.peerdb.flow.jvm;

public class GrpcError extends RuntimeException {
    public GrpcError(String message) {
        super(message);
    }

    public GrpcError(String message, Throwable cause) {
        super(message, cause);
    }
}
