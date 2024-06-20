package com.jimmy.groot.engine.process;

import com.jimmy.groot.engine.base.Process;
import com.jimmy.groot.engine.core.ConfigLoad;
import com.jimmy.groot.platform.base.Message;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.constant.ConfigConstant;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.serializer.SerializerSupport;
import io.netty.channel.ChannelHandlerContext;

public abstract class CallbackProcess<T> implements Process<T> {

    private final Serializer serializer;

    public CallbackProcess() {
        this.serializer = SerializerSupport.getInstance().get(ConfigLoad.get(ConfigConstant.SERIALIZE_TYPE));
    }

    public abstract Message callback(T t);

    public void process(T t, ChannelHandlerContext channelHandlerContext) {
        Message callback = this.callback(t);
        if (callback != null) {
            Event event = new Event();
            event.setType(callback.type().getCode());
            event.setData(serializer.serialize(callback));
            channelHandlerContext.writeAndFlush(event);
        }
    }
}
