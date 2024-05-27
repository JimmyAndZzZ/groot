package com.jimmy.groot.engine.segment;

import com.jimmy.groot.engine.base.Segment;
import lombok.Getter;

public abstract class BaseSegment implements Segment {

    @Getter
    private final int index;

    BaseSegment(int index) {
        this.index = index;
    }
}
