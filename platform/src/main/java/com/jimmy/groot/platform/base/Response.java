package com.jimmy.groot.platform.base;

public interface Response extends Message {

    Long getTraceId();

    Boolean isSuccess();

    String getErrorMessage();
}
