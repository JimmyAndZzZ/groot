package com.jimmy.groot.platform.netty.codec;

import com.jimmy.groot.platform.base.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class NettyEncoder extends MessageToByteEncoder {

    private final Serializer serializer;
    private final Class<?> genericClass;

    public NettyEncoder(Serializer serializer, Class<?> genericClass) {
        this.serializer = serializer;
        this.genericClass = genericClass;
    }

    /**
     * 将对象转换为字节码然后写入到 ByteBuf 对象中
     *
     * @param channelHandlerContext 解码器关联的 ChannelHandlerContext 对象
     * @param o                     编码前的业务对象
     * @param byteBuf               存储编码后的字节数组
     * @throws Exception
     */
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object o, ByteBuf byteBuf) throws Exception {
        if (genericClass.isInstance(o)) {
            //将对象转换为byte数组
            byte[] bytes = serializer.serialize(o);
            //获取消息长度
            int length = bytes.length;
            //写入消息对应的字节数组长度
            byteBuf.writeInt(length);
            //将字节数组写入到bytebuf中
            byteBuf.writeBytes(bytes);
        }
    }
}
