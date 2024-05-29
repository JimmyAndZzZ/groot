package com.jimmy.groot.engine.data.lsm;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.jimmy.groot.engine.enums.TableDataTypeEnum;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmStore {

    private static final int COMPACT_SIZE = 3;

    private static final int DEFAULT_BLOOM_EXPECTED = 1000;

    private long partSize;

    private String dataDir;

    private String tableName;

    private int storeThreshold;

    private ReadWriteLock indexLock;

    private LinkedList<SsTable> ssTables;

    private BloomFilter<String> bloomFilter;

    private TreeMap<String, TableData> index;

    private TreeMap<String, TableData> immutableIndex;

    private LsmStore() {

    }

    public static LsmStore build(String dataDir, String tableName, int storeThreshold, int partSize, int expectCount) {
        LsmStore lsmStore = new LsmStore();
        lsmStore.dataDir = dataDir;
        lsmStore.partSize = partSize;
        lsmStore.tableName = tableName;
        lsmStore.index = new TreeMap<>();
        lsmStore.ssTables = new LinkedList<>();
        lsmStore.storeThreshold = storeThreshold;
        lsmStore.indexLock = new ReentrantReadWriteLock();
        lsmStore.bloomFilter = BloomFilter.create(Funnels.stringFunnel(java.nio.charset.Charset.defaultCharset()), Math.max(expectCount, DEFAULT_BLOOM_EXPECTED), 0.01);

        if (!FileUtil.exist(dataDir)) {
            FileUtil.mkdir(dataDir);
        }

        return lsmStore;
    }

    public void compact() {
        if (CollUtil.isEmpty(ssTables) || ssTables.size() <= COMPACT_SIZE) {
            return;
        }

        indexLock.writeLock().lock();
        try {
            LinkedList<SsTable> compact = new LinkedList<>();
            List<List<SsTable>> split = CollUtil.split(ssTables, COMPACT_SIZE);

            for (int i = split.size() - 1; i >= 0; i--) {
                List<SsTable> tables = split.get(i);
                TreeMap<String, TableData> merge = new TreeMap<>();
                //倒序合并
                for (int j = tables.size() - 1; j >= 0; j--) {
                    SsTable ssTable = tables.get(j);
                    merge.putAll(ssTable.load());
                }
                //ssTable按照时间命名，这样可以保证名称递增
                SsTable ssTable = SsTable.build(dataDir + System.currentTimeMillis() + tableName, partSize);
                ssTable.write(merge);
                compact.addFirst(ssTable);
            }

            this.ssTables = compact;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void set(String key, String columnName, String value) {
        bloomFilter.put(key);

        TableData tableData = new TableData(columnName, value, TableDataTypeEnum.SET);
        this.index.put(key, tableData);
        //内存表大小超过阈值进行持久化
        if (index.size() > storeThreshold) {
            switchIndex();
            storeToSsTable();
        }
    }

    public String get(String key) {
        indexLock.readLock().lock();

        try {
            //优先从布隆过滤器判断
            if (!bloomFilter.mightContain(key)) {
                return null;
            }
            //先从索引中取
            TableData tableData = index.get(key);
            //再尝试从不可变索引中取，此时可能处于持久化ssTable的过程中
            if (tableData == null && immutableIndex != null) {
                tableData = immutableIndex.get(key);
            }
            if (tableData == null) {
                //索引中没有尝试从ssTable中获取，从新的ssTable找到老的
                for (SsTable ssTable : ssTables) {
                    tableData = ssTable.query(key);
                    if (tableData != null) {
                        break;
                    }
                }
            }

            if (tableData != null) {
                TableDataTypeEnum tableDataType = tableData.getTableDataType();
                switch (tableDataType) {
                    case SET:
                        return tableData.getValue();
                    case REMOVE:
                        return null;
                }
            }

            return null;
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 切换内存表
     */
    private void switchIndex() {
        indexLock.writeLock().lock();
        try {
            //切换内存表
            immutableIndex = index;
            index = new TreeMap<>();
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 保存数据到ssTable
     */
    private void storeToSsTable() {
        //ssTable按照时间命名，这样可以保证名称递增
        SsTable ssTable = SsTable.build(dataDir + System.currentTimeMillis() + tableName, partSize);
        ssTable.write(this.immutableIndex);
        this.ssTables.addFirst(ssTable);
        //持久化完成删除暂存的内存表和WAL_TMP
        this.immutableIndex = null;
    }
}
