package com.jimmy.groot.engine.netty;

import cn.hutool.core.util.StrUtil;
import com.jimmy.groot.engine.core.ConfigLoad;
import com.jimmy.groot.engine.exception.ConnectionException;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.netty.codec.NettyDecoder;
import com.jimmy.groot.platform.netty.codec.NettyEncoder;
import com.jimmy.groot.platform.other.Assert;
import com.jimmy.groot.platform.serializer.KryoSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Client {

    private String server;

    private Integer port;

    private Channel channel;

    private Bootstrap bootstrap;

    private EventLoopGroup group;

    private KryoSerializer kryoSerializer;

    @Getter
    private Boolean connectSuccess = false;

    private AtomicInteger retry = new AtomicInteger(0);

    public Client(String server) {
        List<String> split = StrUtil.split(server, ":");

        Assert.isTrue(split.size() == 2, "配置服务端地址异常");

        this.server = split.get(0);
        this.port = Integer.valueOf(split.get(1));
    }

    public void send(Event event) {
        if (!connectSuccess) {
            throw new ConnectionException();
        }

        this.channel.writeAndFlush(event);
    }


    public void init(ConfigLoad configLoad) {
        bootstrap = new Bootstrap();
        group = new NioEventLoopGroup();
        bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.TCP_NODELAY, Boolean.TRUE).option(ChannelOption.SO_REUSEADDR, Boolean.TRUE).option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT).handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("decoder", new NettyDecoder(kryoSerializer, Event.class));
                pipeline.addLast("encoder", new NettyEncoder(kryoSerializer, Event.class));
                pipeline.addLast(new ClientHandler(configLoad, Client.this));
            }
        });

        connect();
    }

    public void connect() {
        try {
            this.connectSuccess = false;

            ChannelFuture cf = bootstrap.connect(server, port);
            cf.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    //重连交给后端线程执行
                    if (retry.getAndIncrement() <= 30) {
                        // 重连交给后端线程执行
                        future.channel().eventLoop().schedule(this::connect, 10, TimeUnit.SECONDS);
                    }
                } else {
                    retry.set(0);
                    connectSuccess = true;
                }
            });
            //对通道关闭进行监听
            this.channel = cf.sync().channel();
        } catch (InterruptedException exception) {
            throw new EngineException("客户端被中断");
        }
    }
}
