package com.jimmy.groot.platform.serializer;

import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.exception.SerializerException;

import java.util.HashMap;
import java.util.Map;

public class SerializerSupport {

    private final Map<String, Serializer> serializerMap = new HashMap<>();

    private static class SingletonHolder {
        private static final SerializerSupport INSTANCE = new SerializerSupport();
    }

    public static SerializerSupport getInstance() {
        return SerializerSupport.SingletonHolder.INSTANCE;
    }

    private SerializerSupport() {
        this.serializerMap.put("JSON", new JSONSerializer());
        this.serializerMap.put("KRYO", new KryoSerializer());
    }

    public Serializer get(String type) {
        Serializer serializer = this.serializerMap.get(type);
        if (serializer == null) {
            throw new SerializerException(type + "序列化方式不存在");
        }

        return serializer;
    }
}
