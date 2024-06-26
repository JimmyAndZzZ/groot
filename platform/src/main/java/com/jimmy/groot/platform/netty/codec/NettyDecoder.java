package com.jimmy.groot.platform.netty.codec;

import com.jimmy.groot.platform.base.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class NettyDecoder extends ByteToMessageDecoder {

    private static final int BODY_LENGTH = 4;

    private final Class<?> genericClass;

    private final Serializer serializer;

    public NettyDecoder(Serializer serializer, Class<?> genericClass) {
        this.serializer = serializer;
        this.genericClass = genericClass;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        //因为消息长度所占字节为4，则得到的入站数据必须要大于4
        if (byteBuf.readableBytes() >= BODY_LENGTH) {
            //标记当前readIndex的位置，方便后面重置
            byteBuf.readerIndex();
            //读取消息长度
            int dataLength = byteBuf.readInt();
            //情况判断
            if (dataLength < 0 || byteBuf.readableBytes() < 0) {
                return;
            }
            //如果可读字节数小于消息长度的话，说明不是完整的消息，重置ReaderIndex
            if (byteBuf.readableBytes() < dataLength) {
                byteBuf.resetReaderIndex();
                return;
            }
            //到这里就可以正常反序列化了
            byte[] body = new byte[dataLength];
            byteBuf.readBytes(body);
            Object object = serializer.deserialize(body, genericClass);
            list.add(object);
        }
    }
}
