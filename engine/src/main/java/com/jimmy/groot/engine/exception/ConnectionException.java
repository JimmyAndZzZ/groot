package com.jimmy.groot.engine.exception;

public class ConnectionException extends RuntimeException {

    public ConnectionException() {
        super("服务端不可用");
    }
}
