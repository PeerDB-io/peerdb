package io.peerdb.flow.jvm;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.ExceptionHandler;
import io.quarkus.grpc.ExceptionHandlerProvider;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DefaultExceptionHandlerProvider implements ExceptionHandlerProvider {
    public static boolean invoked;

    private static Exception toStatusException(Throwable t) {
        return Status.fromThrowable(t).withDescription(t.getMessage()).asRuntimeException();
    }

    @Override
    public <ReqT, RespT> ExceptionHandler<ReqT, RespT> createHandler(ServerCall.Listener<ReqT> listener,
                                                                     ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
        return new DefaultExceptionHandler<>(listener, serverCall, metadata);
    }

    @Override
    public Throwable transform(Throwable t) {
        invoked = true;
        Log.errorf(t, "Received error in gRPC call: '%s'", t.getMessage());
        return toStatusException(t);
    }

    private static class DefaultExceptionHandler<A, B> extends ExceptionHandler<A, B> {
        public DefaultExceptionHandler(ServerCall.Listener<A> listener, ServerCall<A, B> call, Metadata metadata) {
            super(listener, call, metadata);
        }

        @Override
        protected void handleException(Throwable t, ServerCall<A, B> call, Metadata metadata) {
            invoked = true;
            StatusRuntimeException sre = (StatusRuntimeException) ExceptionHandlerProvider.toStatusException(t, true);
            Metadata trailers = sre.getTrailers() != null ? sre.getTrailers() : metadata;
            call.close(sre.getStatus(), trailers);
        }
    }
}

