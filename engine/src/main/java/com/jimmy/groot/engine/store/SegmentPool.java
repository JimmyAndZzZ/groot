package com.jimmy.groot.engine.store;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.jimmy.groot.engine.base.Segment;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.engine.other.Assert;
import com.jimmy.groot.engine.other.IntObjectHashMap;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SegmentPool {

    private final static int DEFAULT_CAPACITY = 32;

    private final AtomicInteger counter = new AtomicInteger(0);

    private final SegmentQueue wait = new SegmentQueue();

    private final IntObjectHashMap<Segment> pool = new IntObjectHashMap<>(216);

    private static class SingletonHolder {
        private static final SegmentPool INSTANCE = new SegmentPool();
    }

    public static SegmentPool getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private SegmentPool() {
        new Thread(() -> {
            while (true) {
                if (wait.getLastPollTimestamp() < System.currentTimeMillis() - 60 * 1000) {
                    Segment poll = wait.poll();
                    if (poll != null && poll.isFree()) {
                        poll.release();
                        poll = null;
                    }
                }

                ThreadUtil.sleep(1000);
            }
        }).start();
    }

    public List<Integer> allocateFromMemory(byte[] bytes) {
        try {
            bytes = Snappy.compress(bytes);
            //压缩
            List<Integer> indexes = Lists.newArrayList();
            //切割
            List<byte[]> spilt = this.splitByteArray(bytes);

            for (byte[] b : spilt) {
                Segment poll = wait.poll();
                if (poll != null && poll.isFree() && poll.write(b)) {
                    indexes.add(poll.getIndex());
                    continue;
                }

                Segment memory = new HeapMemorySegment(DEFAULT_CAPACITY, counter.incrementAndGet());
                memory.write(b);

                pool.put(memory.getIndex(), memory);
                indexes.add(memory.getIndex());
            }

            return indexes;
        } catch (Exception e) {
            throw new EngineException(e.getMessage());
        }
    }

    public Integer allocateFromDisk(byte[] bytes) {
        Segment disk = new DiskSegment(counter.incrementAndGet());
        disk.write(bytes);

        pool.put(disk.getIndex(), disk);
        return disk.getIndex();
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

            Assert.notNull(bytes, "数组为空");

            if (indices.size() == 1) {
                return Snappy.uncompress(bytes);
            }

            for (int i = 1; i < indices.size(); i++) {
                byte[] other = this.get(indices.get(i));
                Assert.notNull(other, "数组为空");

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
        Segment segment = pool.get(index);
        if (segment != null) {
            segment.free();

            if (segment.isNeedRecycle()) {
                wait.add(segment);
            }
        }
    }

    public byte[] get(Integer index) {
        return pool.get(index).read();
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
