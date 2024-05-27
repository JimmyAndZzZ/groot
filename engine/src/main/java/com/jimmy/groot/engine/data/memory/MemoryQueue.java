package com.jimmy.groot.engine.data.memory;

import lombok.Getter;

import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class MemoryQueue extends ConcurrentLinkedQueue<MemorySegment> {

    private long lastPollTimestamp;

    @Override
    public MemorySegment poll() {
        this.lastPollTimestamp = System.currentTimeMillis();
        return super.poll();
    }

}
