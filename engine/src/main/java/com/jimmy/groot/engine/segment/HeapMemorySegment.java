package com.jimmy.groot.engine.segment;

import com.jimmy.groot.platform.other.Assert;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeapMemorySegment extends BaseSegment {

    private ByteBuffer byteBuffer;

    private final AtomicBoolean free = new AtomicBoolean(true);

    HeapMemorySegment(int capacity, int index) {
        super(index);
        this.byteBuffer = ByteBuffer.allocate(capacity);
    }

    @Override
    public boolean isFree() {
        return free.get();
    }

    @Override
    public boolean isNeedRecycle() {
        return true;
    }

    @Override
    public void release() {
        byteBuffer = null;
    }

    @Override
    public boolean write(byte[] bytes) {
        if (!free.compareAndSet(true, false)) {
            return false;
        }

        this.byteBuffer.put(bytes);
        return true;
    }

    @Override
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

    @Override
    public void free() {
        Assert.isTrue(free.compareAndSet(false, true), "该内存块空闲");
        byteBuffer.clear();
    }
}
