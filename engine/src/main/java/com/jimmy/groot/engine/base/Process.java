package com.jimmy.groot.engine.base;

import io.netty.channel.ChannelHandlerContext;

public interface Process<T> {

    void process(T t, ChannelHandlerContext channelHandlerContext);
}
