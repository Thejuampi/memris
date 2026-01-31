package io.memris.core;

public class MemrisException extends RuntimeException {

    public MemrisException(Throwable cause) {
        super(cause);
    }

    public MemrisException(String message, Throwable cause) {
        super(message, cause);
    }

    public MemrisException(String message) {
        super(message);
    }

}
