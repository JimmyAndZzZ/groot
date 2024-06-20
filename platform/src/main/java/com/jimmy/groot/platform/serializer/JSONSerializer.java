package com.jimmy.groot.platform.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.exception.SerializerException;

import java.io.IOException;

public class JSONSerializer implements Serializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Object o) throws SerializerException {
        if (o == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new SerializerException("序列化失败", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) throws SerializerException {
        if (bytes == null) {
            return null;
        }

        try {
            return objectMapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new SerializerException("反序列化失败", e);
        }
    }
}
