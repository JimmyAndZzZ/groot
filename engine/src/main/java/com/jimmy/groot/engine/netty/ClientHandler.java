package com.jimmy.groot.engine.netty;

import com.jimmy.groot.engine.core.ConfigLoad;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.enums.EventTypeEnum;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@ChannelHandler.Sharable
public class ClientHandler extends SimpleChannelInboundHandler<Event> {

    private final Map<EventTypeEnum, Class<?>> classMap = new HashMap<>();

    private final Map<EventTypeEnum, Process<?>> processMap = new HashMap<>();

    private final ExecutorService executorService = new ThreadPoolExecutor(10, 60,
            60L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private Client client;

    private ConfigLoad configLoad;

    public ClientHandler(ConfigLoad configLoad, Client client) throws Exception {
        super();
        this.configLoad = configLoad;
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Event event) throws Exception {
        String type = event.getType();
        String message = event.getMessage();

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
