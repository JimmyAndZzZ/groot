package com.jimmy.groot.center.netty;

import com.jimmy.groot.center.core.ConfigLoad;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.constant.ConfigConstant;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.netty.codec.NettyDecoder;
import com.jimmy.groot.platform.netty.codec.NettyEncoder;
import com.jimmy.groot.platform.serializer.SerializerSupport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CenterSever {

    private final ExecutorService executorService = new ThreadPoolExecutor(
            10,
            120,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());

    public void boot() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup boss = new NioEventLoopGroup();
        EventLoopGroup worker = new NioEventLoopGroup();

        Serializer serializer = SerializerSupport.getInstance().get(ConfigLoad.get(ConfigConstant.SERIALIZE_TYPE));

        String port = ConfigLoad.get(ConfigConstant.Center.BOOT_PORT);

        try {
            bootstrap.group(boss, worker)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
                    .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                    .childOption(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            channel.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(4096));
                            pipeline.addLast("decoder", new NettyDecoder(serializer, Event.class));
                            pipeline.addLast("encoder", new NettyEncoder(serializer, Event.class));
                            pipeline.addLast(new CenterEventHandler(actionSupport, executorService));

                        }
                    });
            ChannelFuture f = bootstrap.bind(Integer.parseInt(port)).sync();

            log.info("gateway server start port:{}", port);

            f.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("服务端启动失败", e);
        } finally {
            worker.shutdownGracefully();
            boss.shutdownGracefully();
        }
    }

    public void close() {
        ChannelHandlerPool.close();
        executorService.shutdown();
    }
}
