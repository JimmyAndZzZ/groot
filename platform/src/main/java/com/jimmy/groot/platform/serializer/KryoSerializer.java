package com.jimmy.groot.platform.serializer;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.serializers.MapSerializer;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.exception.SerializerException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;

public class KryoSerializer implements Serializer {

    //由于Kryo是线程不安全的，所以我们这里使用ThreadLocal来解决线程安全问题
    public static ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(HashMap.class, new MapSerializer());
        kryo.register(Timestamp.class);
        kryo.register(BigInteger.class);
        kryo.register(BigDecimal.class);
        kryo.reference(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo;
    });

    @Override
    public byte[] serialize(Object obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final Output output = new Output(baos, 8192)) {
            Kryo kryo = kryoThreadLocal.get();
            //进行序列化
            kryo.writeObject(output, obj);
            kryoThreadLocal.remove();
            return output.toBytes();
        } catch (Exception e) {
            throw new SerializerException("序列化失败", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        if (bytes == null) {
            return null;
        }

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
             Input input = new Input(byteArrayInputStream, 8192)) {
            Kryo kryo = kryoThreadLocal.get();
            T obj = kryo.readObject(input, clazz);
            kryoThreadLocal.remove();
            return obj;
        } catch (Exception e) {
            throw new SerializerException("反序列化失败", e);
        }
    }
}
