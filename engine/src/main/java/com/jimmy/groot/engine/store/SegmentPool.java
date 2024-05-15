package com.jimmy.groot.engine.store;


import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.jimmy.groot.sql.exception.EngineException;
import io.netty.util.collection.IntObjectHashMap;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SegmentPool {

    private final static int DEFAULT_CAPACITY = 32;

    private final AtomicInteger counter = new AtomicInteger(0);

    private final IntObjectHashMap<Segment> segmentPool = new IntObjectHashMap<>(216);

    private static class SingletonHolder {
        private static final SegmentPool INSTANCE = new SegmentPool();
    }

    public static SegmentPool getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private SegmentPool() {

    }

    public List<Integer> allocate(byte[] bytes) {
        try {
            bytes = Snappy.compress(bytes);
            //压缩
            List<Integer> index = Lists.newArrayList();
            //切割
            List<byte[]> spilt = this.splitByteArray(bytes);

            for (byte[] b : spilt) {
                Segment segment = new Segment(DEFAULT_CAPACITY);
                segment.write(b);
                int l = this.counter.incrementAndGet();
                this.segmentPool.put(l, segment);
                index.add(l);
            }

            return index;
        } catch (Exception e) {
            throw new EngineException(e.getMessage());
        }
    }

    public byte[] getAndFree(List<Integer> indices) {
        byte[] bytes = this.get(indices);
        this.free(indices);
        return bytes;
    }

    public byte[] get(List<Integer> indices) {
        try {
            if (CollUtil.isEmpty(indices)) {
                return null;
            }

            Integer first = indices.get(0);
            byte[] bytes = this.get(first);
            if (bytes == null) {
                throw new EngineException("数组为空");
            }

            if (indices.size() == 1) {
                return Snappy.uncompress(bytes);
            }

            for (int i = 1; i < indices.size(); i++) {
                byte[] other = this.get(indices.get(i));
                if (other == null) {
                    throw new EngineException("数组为空");
                }

                bytes = this.mergeByteArray(bytes, other);
            }

            return Snappy.uncompress(bytes);
        } catch (Exception e) {
            throw new EngineException(e.getMessage());
        }
    }

    public void free(Collection<Integer> indices) {
        if (CollUtil.isEmpty(indices)) {
            return;
        }

        for (Integer index : indices) {
            this.free(index);
        }
    }

    public void free(Integer index) {
        Segment segment = this.segmentPool.remove(index);
        if (segment != null) {
            segment.free();
        }
    }

    /**
     * 根据下标获取数据
     *
     * @param index
     * @return
     */
    private byte[] get(Integer index) {
        return this.segmentPool.get(index).read();
    }

    /**
     * 切割byte数组
     *
     * @param array
     * @return
     */
    private List<byte[]> splitByteArray(byte[] array) {
        int amount = array.length / DEFAULT_CAPACITY;
        List<byte[]> split = Lists.newLinkedList();
        if (amount == 0) {
            split.add(array);
            return split;
        }
        //判断余数
        int remainder = array.length % DEFAULT_CAPACITY;
        if (remainder != 0) {
            ++amount;
        }

        byte[] arr;
        for (int i = 0; i < amount; i++) {
            if (i == amount - 1 && remainder != 0) {
                // 有剩余，按照实际长度创建
                arr = new byte[remainder];
                System.arraycopy(array, i * DEFAULT_CAPACITY, arr, 0, remainder);
            } else {
                arr = new byte[DEFAULT_CAPACITY];
                System.arraycopy(array, i * DEFAULT_CAPACITY, arr, 0, DEFAULT_CAPACITY);
            }

            split.add(arr);
        }
        return split;
    }

    /**
     * 合并byte数组
     *
     * @param bt1
     * @param bt2
     * @return
     */
    private byte[] mergeByteArray(byte[] bt1, byte[] bt2) {
        byte[] bt3 = new byte[bt1.length + bt2.length];
        System.arraycopy(bt1, 0, bt3, 0, bt1.length);
        System.arraycopy(bt2, 0, bt3, bt1.length, bt2.length);
        return bt3;
    }
}
