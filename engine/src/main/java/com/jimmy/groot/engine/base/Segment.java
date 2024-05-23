package com.jimmy.groot.engine.base;

public interface Segment {

    boolean write(byte[] bytes);

    byte[] read();

    void free();

    boolean isFree();

    boolean isNeedRecycle();

    int getIndex();

    void release();
}
