package com.jimmy.groot.engine.store;

import com.jimmy.groot.engine.base.Segment;
import lombok.Getter;

import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class SegmentQueue extends ConcurrentLinkedQueue<Segment> {

    private long lastPollTimestamp;

    @Override
    public Segment poll() {
        this.lastPollTimestamp = System.currentTimeMillis();
        return super.poll();
    }

}
