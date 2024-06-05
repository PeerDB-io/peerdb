package io.peerdb.flow.jvm;


import io.grpc.*;
import io.quarkus.grpc.GlobalInterceptor;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@GlobalInterceptor
public class ErrorInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Log.infof("Intercepting call", metadata);
        System.out.println("Intercepting call sout");

        ServerCall<ReqT, RespT> listener = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
            @Override
            public void sendMessage(RespT message) {
                Log.debugf("Sending message for method: %s: {%s}", serverCall.getMethodDescriptor().getFullMethodName(), message);
                super.sendMessage(message);
            }
        };

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(serverCallHandler.startCall(listener, metadata)) {

            @Override
            public void onMessage(ReqT message) {
                Log.debugf("Received message from client for method: %s: {%s}", serverCall.getMethodDescriptor().getFullMethodName(), message);
                super.onMessage(message);
            }

        };

//        return serverCallHandler.startCall(serverCall, metadata);
    }
}
