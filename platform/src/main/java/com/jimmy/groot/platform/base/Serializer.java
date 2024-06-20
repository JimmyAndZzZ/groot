package com.jimmy.groot.platform.base;

import com.jimmy.groot.platform.exception.SerializerException;

public interface Serializer {

    byte[] serialize(Object o) throws SerializerException;

    <T> T deserialize(byte[] bytes, Class<T> clazz) throws SerializerException;
}
