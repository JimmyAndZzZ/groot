package com.jimmy.groot.engine.data.memory;

import lombok.Getter;

import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class SegmentQueue extends ConcurrentLinkedQueue<HeapMemorySegment> {

    private long lastPollTimestamp;

    @Override
    public HeapMemorySegment poll() {
        this.lastPollTimestamp = System.currentTimeMillis();
        return super.poll();
    }

}
