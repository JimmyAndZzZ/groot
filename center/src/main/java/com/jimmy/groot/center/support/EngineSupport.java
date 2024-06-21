package com.jimmy.groot.center.support;

import com.google.common.collect.Maps;
import com.jimmy.groot.center.exception.CenterException;
import com.jimmy.groot.center.netty.ChannelHandlerPool;
import com.jimmy.groot.platform.base.Message;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.core.center.Engine;
import io.netty.channel.Channel;

import java.util.Map;

public class EngineSupport {

    private final Map<String, Engine> engineCollect = Maps.newConcurrentMap();

    private Serializer serializer;

    public void send(String id, Message message) {
        Channel channel = ChannelHandlerPool.getChannel(id);
        if (channel == null) {
            throw new CenterException("engine not connect");
        }

        Event event = new Event();
        event.setType(message.type().getCode());
        event.setData(serializer.serialize(message));
        channel.writeAndFlush(event);
    }


    public <T> T invoke(String id, Message message, Class<T> clazz) {

    }
}
