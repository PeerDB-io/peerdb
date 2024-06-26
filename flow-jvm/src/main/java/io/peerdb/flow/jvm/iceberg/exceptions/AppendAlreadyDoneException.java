package io.peerdb.flow.jvm.iceberg.exceptions;

public class AppendAlreadyDoneException extends RuntimeException {
    public AppendAlreadyDoneException() {
        super();
    }

    public AppendAlreadyDoneException(String message) {
        super(message);
    }
}
