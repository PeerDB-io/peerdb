package io.peerdb.flow.jvm;


import com.google.common.base.Stopwatch;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.TimeUnit;

@ApplicationScoped
@GlobalInterceptor
public class RequestLoggingInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        var stopwatch = Stopwatch.createStarted();
        Log.infof("Received request for method {%s}", serverCall.getMethodDescriptor().getFullMethodName());
        ServerCall<ReqT, RespT> listener = new ForwardingServerCall.SimpleForwardingServerCall<>(serverCall) {
        };
        return new CallListener<>(serverCallHandler, listener, metadata, serverCall, stopwatch);

    }

    private static class CallListener<ReqT, RespT> extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

        private final ServerCall<ReqT, RespT> serverCall;
        private final Stopwatch stopwatch;

        public CallListener(ServerCallHandler<ReqT, RespT> serverCallHandler, ServerCall<ReqT, RespT> listener, Metadata metadata, ServerCall<ReqT, RespT> serverCall, Stopwatch stopwatch) {
            super(serverCallHandler.startCall(listener, metadata));
            this.serverCall = serverCall;
            this.stopwatch = stopwatch;
        }

        @Override
        public void onMessage(ReqT message) {
//            Log.debugf("Received request for method: %s: {%s}", serverCall.getMethodDescriptor().getFullMethodName(), message);
            super.onMessage(message);
        }

        @Override
        public void onComplete() {
            Log.infof("Call completed for method: {%s} in %d ms", serverCall.getMethodDescriptor().getFullMethodName(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            super.onComplete();
        }

    }
}
