package com.jimmy.groot.center.netty;

import com.jimmy.groot.center.action.ActionSupport;
import com.jimmy.groot.center.core.DestroyHook;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.core.Event;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class CenterEventHandler extends SimpleChannelInboundHandler<Event> {

    private final ActionSupport actionSupport;

    private final ExecutorService executorService;

    public CenterEventHandler(Serializer serializer) throws Exception {
        this.actionSupport = new ActionSupport(serializer);
        this.executorService = new ThreadPoolExecutor(
                10,
                120,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());

        DestroyHook.registerHook(executorService::shutdown);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Event event) throws Exception {
        try {
            executorService.submit(() -> actionSupport.action(event, ctx));
        } catch (RejectedExecutionException e) {
            log.error("Thread Pool Full");
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //关闭与客户端的连接
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("事件处理异常", cause);
    }
}
