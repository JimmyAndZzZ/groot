package com.jimmy.groot.engine.process;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ClassLoaderUtil;
import cn.hutool.core.util.ClassUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.groot.engine.base.Process;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.core.Event;
import com.jimmy.groot.platform.enums.EventTypeEnum;
import com.jimmy.groot.platform.exception.SerializerException;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ProcessSupport implements Process<Event> {

    private final Map<EventTypeEnum, Class<?>> classMap = new HashMap<>();

    private final Map<EventTypeEnum, Process<?>> processMap = new HashMap<>();

    private final Serializer serializer;

    public ProcessSupport(Serializer serializer) throws InstantiationException, IllegalAccessException {
        this.serializer = serializer;

        Set<Class<?>> classes = ClassUtil.scanPackage(this.getClass().getPackage().getName());
        if (CollUtil.isNotEmpty(classes)) {
            for (Class<?> clazz : classes) {
                EventTypeEnum eventTypeEnum = EventTypeEnum.queryByCode(StrUtil.removeAll(clazz.getName(), Process.class.getName()));
                if (eventTypeEnum == null) {
                    continue;
                }

                if (clazz.equals(this.getClass())) {
                    continue;
                }

                if (ClassUtil.isAbstractOrInterface(clazz)) {
                    continue;
                }

                if (!this.isProcess(clazz)) {
                    continue;
                }

                processMap.put(eventTypeEnum, (Process<?>) clazz.newInstance());

                Type[] genericInterfaces = clazz.getGenericInterfaces();
                if (ArrayUtil.isNotEmpty(genericInterfaces)) {
                    Type genericInterface = genericInterfaces[0];
                    // 如果gType类型是ParameterizedType对象
                    if (genericInterface instanceof ParameterizedType) {
                        // 强制类型转换
                        ParameterizedType pType = (ParameterizedType) genericInterface;
                        // 取得泛型类型的泛型参数
                        Type[] tArgs = pType.getActualTypeArguments();
                        if (ArrayUtil.isNotEmpty(tArgs)) {
                            classMap.put(eventTypeEnum, ClassUtil.loadClass(tArgs[0].getTypeName()));
                        }
                    }
                }
            }
        }
    }

    @Override
    public void process(Event event, ChannelHandlerContext channelHandlerContext) {
        String type = event.getType();
        byte[] data = event.getData();

        EventTypeEnum eventTypeEnum = EventTypeEnum.queryByCode(type);
        if (eventTypeEnum == null) {
            return;
        }

        Class<?> clazz = classMap.get(eventTypeEnum);
        Process process = processMap.get(eventTypeEnum);

        if (process == null || clazz == null) {
            return;
        }

        try {
            process.process(serializer.deserialize(data, clazz), channelHandlerContext);
        } catch (SerializerException e) {
            log.error("反序列化失败", e.getE());
        }
    }

    /**
     * 判断是否是process类
     *
     * @param clazz
     * @return
     */
    private boolean isProcess(Class<?> clazz) {
        Class<?>[] interfaces = clazz.getInterfaces();
        if (ArrayUtil.isNotEmpty(interfaces)) {
            for (Class<?> anInterface : interfaces) {
                if (anInterface.equals(Process.class)) {
                    return true;
                }
            }
        }

        Class<?> superclass = clazz.getSuperclass();
        return superclass != null && this.isProcess(superclass);
    }
}
