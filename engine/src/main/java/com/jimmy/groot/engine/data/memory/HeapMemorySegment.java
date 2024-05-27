package com.jimmy.groot.engine.data.memory;

import com.jimmy.groot.platform.other.Assert;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeapMemorySegment {

    private ByteBuffer byteBuffer;

    @Getter
    private final int index;

    private final AtomicBoolean free = new AtomicBoolean(true);

    HeapMemorySegment(int capacity, int index) {
        this.index = index;
        this.byteBuffer = ByteBuffer.allocate(capacity);
    }

    public boolean isFree() {
        return free.get();
    }

    public void release() {
        byteBuffer = null;
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

    public void free() {
        Assert.isTrue(free.compareAndSet(false, true), "该内存块空闲");
        byteBuffer.clear();
    }
}
