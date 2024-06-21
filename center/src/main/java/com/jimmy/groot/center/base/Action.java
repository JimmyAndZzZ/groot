package com.jimmy.groot.center.base;

import io.netty.channel.ChannelHandlerContext;

public interface Action<T> {

    void action(T t, ChannelHandlerContext channelHandlerContext);
}
