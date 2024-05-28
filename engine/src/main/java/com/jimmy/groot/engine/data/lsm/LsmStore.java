package com.jimmy.groot.engine.data.lsm;

import cn.hutool.core.io.FileUtil;
import com.jimmy.groot.engine.enums.TableDataTypeEnum;

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmStore {

    /**
     * 数据分区大小
     */
    private long partSize;

    /**
     * 数据目录
     */
    private String dataDir;

    /**
     * lsm表名
     */
    private String tableName;

    /**
     * 持久化阈值
     */
    private int storeThreshold;

    /**
     * 读写锁
     */
    private ReadWriteLock indexLock;

    /**
     * ssTable列表
     */
    private LinkedList<SsTable> ssTables;

    /**
     * 内存表
     */
    private TreeMap<String, TableData> index;

    /**
     * 不可变内存表，用于持久化内存表中时暂存数据
     */
    private TreeMap<String, TableData> immutableIndex;

    private LsmStore() {

    }

    public static LsmStore build(String dataDir, String tableName, int storeThreshold, int partSize) {
        LsmStore lsmStore = new LsmStore();
        lsmStore.dataDir = dataDir;
        lsmStore.partSize = partSize;
        lsmStore.tableName = tableName;
        lsmStore.index = new TreeMap<>();
        lsmStore.ssTables = new LinkedList<>();
        lsmStore.storeThreshold = storeThreshold;
        lsmStore.indexLock = new ReentrantReadWriteLock();

        if (!FileUtil.exist(dataDir)) {
            FileUtil.mkdir(dataDir);
        }

        return lsmStore;
    }

    public void set(String key, String columnName, String value) {
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
        try {
            indexLock.writeLock().lock();
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
