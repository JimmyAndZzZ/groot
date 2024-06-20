package com.jimmy.groot.platform.exception;

import lombok.Getter;

@Getter
public class SerializerException extends RuntimeException {

    private final Exception e;

    public SerializerException(String message, Exception e) {
        super(message);
        this.e = e;
    }
}
