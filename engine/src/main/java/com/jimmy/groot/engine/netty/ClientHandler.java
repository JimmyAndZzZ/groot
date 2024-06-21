package com.jimmy.groot.engine.netty;

import com.jimmy.groot.engine.core.DestroyHook;
import com.jimmy.groot.engine.process.ProcessSupport;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.core.Event;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
@ChannelHandler.Sharable
public class ClientHandler extends SimpleChannelInboundHandler<Event> {

    private final Client client;

    private final ProcessSupport processSupport;

    private final ExecutorService executorService;

    public ClientHandler(Client client, Serializer serializer) throws Exception {
        super();
        this.client = client;
        this.processSupport = new ProcessSupport(serializer);
        this.executorService = new ThreadPoolExecutor(10, 60,
                60L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        DestroyHook.registerHook(executorService::shutdown);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Event event) throws Exception {
        try {
            executorService.execute(() -> processSupport.process(event, ctx));
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
