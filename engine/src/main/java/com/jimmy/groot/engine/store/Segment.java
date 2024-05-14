package com.jimmy.groot.engine.store;

import org.springframework.util.Assert;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class Segment {

    private final AtomicBoolean free;

    private final ByteBuffer byteBuffer;

    Segment(int capacity) {
        this.free = new AtomicBoolean(true);
        this.byteBuffer = ByteBuffer.allocate(capacity);
    }

    public boolean write(byte[] bytes) {
        if (!free.compareAndSet(true, false)) {
            return false;
        }

        this.byteBuffer.put(bytes);
        return true;
    }

    public byte[] read() {
        Assert.isTrue(!free.get(), "该内存块空闲");

        this.byteBuffer.flip();
        int len = this.byteBuffer.limit() - this.byteBuffer.position();

        byte[] bytes = new byte[len];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = this.byteBuffer.get();
        }

        return bytes;
    }

    public boolean isFree() {
        return free.get();
    }

    public void free() {
        Assert.isTrue(free.compareAndSet(false, true), "该内存块空闲");
        byteBuffer.clear();
    }
}
