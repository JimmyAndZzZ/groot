package com.jimmy.groot.platform.netty.codec;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
public class NettySerializer {

    public static final ThreadLocal<Kryo> KryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setReferences(true);
        kryo.setRegistrationRequired(false);
        return kryo;
    });


    /**
     * 序列化
     *
     * @param obj 要序列化的对象
     * @return
     */
    public byte[] serialize(Object obj) throws IOException {
        Kryo kryo = KryoThreadLocal.get();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(); Output output = new Output(byteArrayOutputStream)) {
            // Object->byte: 将对象序列化为 byte 数组
            kryo.writeObject(output, obj);
            KryoThreadLocal.remove();
            return output.toBytes();
        }
    }

    /**
     * 反序列化
     *
     * @param bytes 序列化后的字节数组
     * @param clazz clazz 类
     * @param <T>
     * @return
     */
    public <T> T deserialize(byte[] bytes, Class<T> clazz) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes); Input input = new Input(byteArrayInputStream)) {
            Kryo kryo = KryoThreadLocal.get();
            // byte->Object: 从 byte 数组中反序列化出对象
            Object o = kryo.readObject(input, clazz);
            KryoThreadLocal.remove();
            return clazz.cast(o);
        }
    }
}
