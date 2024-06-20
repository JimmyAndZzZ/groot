package com.jimmy.groot.engine.netty;

import com.jimmy.groot.engine.core.ConfigLoad;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.enums.EventTypeEnum;
import com.jimmy.groot.platform.exception.SerializerException;
import com.jimmy.groot.platform.serializer.JSONSerializer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import com.jimmy.groot.engine.base.Process;

@Slf4j
@ChannelHandler.Sharable
public class ClientHandler extends SimpleChannelInboundHandler<Event> {

    private final Map<EventTypeEnum, Class<?>> classMap = new HashMap<>();

    private final Map<EventTypeEnum, Process<?>> processMap = new HashMap<>();

    private final ExecutorService executorService = new ThreadPoolExecutor(10, 60,
            60L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private final Client client;

    private final Serializer serializer;

    public ClientHandler(Client client, Serializer serializer) throws Exception {
        super();
        this.client = client;
        this.serializer = serializer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Event event) throws Exception {
        String type = event.getType();
        byte[] data = event.getData();

        EventTypeEnum eventTypeEnum = EventTypeEnum.queryByCode(type);
        if (eventTypeEnum == null) {
            return;
        }

        try {
            executorService.execute(() -> {
                Class<?> clazz = classMap.get(eventTypeEnum);
                Process process = processMap.get(eventTypeEnum);

                if (process == null || clazz == null) {
                    return;
                }

                try {
                    process.process(serializer.deserialize(data, clazz), ctx);
                } catch (SerializerException e) {
                    log.error("反序列化失败");
                }
            });
        } catch (RejectedExecutionException e) {
            log.error("Thread Pool Full");
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        client.connect();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("事件处理异常", cause);
    }
}
