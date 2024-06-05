package com.jimmy.groot.engine.data.lsm;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.jimmy.groot.engine.enums.TableDataTypeEnum;
import com.jimmy.groot.engine.exception.EngineException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LsmPartition {

    private static final String WAL = "wal";

    private static final int COMPACT_SIZE = 3;

    private static final String RW_MODE = "rw";

    private static final String TABLE = ".table";

    private static final String WAL_TMP = "walTmp";

    private static final int DEFAULT_BLOOM_EXPECTED = 1000;

    private File walFile;

    private long partSize;

    private String dataDir;

    private int storeThreshold;

    private RandomAccessFile wal;

    private ReadWriteLock indexLock;

    private ObjectMapper objectMapper;

    private LinkedList<SsTable> ssTables;

    private BloomFilter<String> bloomFilter;

    private TreeMap<String, TableData> index;

    private TreeMap<String, TableData> immutableIndex;

    private LsmPartition() {
    }

    public static LsmPartition build(String dataDir, int storeThreshold, long partSize, int expectCount) {
        try {
            LsmPartition lsmStore = new LsmPartition();
            lsmStore.dataDir = dataDir;
            lsmStore.partSize = partSize;
            lsmStore.index = new TreeMap<>();
            lsmStore.ssTables = new LinkedList<>();
            lsmStore.storeThreshold = storeThreshold;
            lsmStore.objectMapper = new ObjectMapper();
            lsmStore.indexLock = new ReentrantReadWriteLock();
            lsmStore.bloomFilter = BloomFilter.create(Funnels.stringFunnel(java.nio.charset.Charset.defaultCharset()), Math.max(expectCount, DEFAULT_BLOOM_EXPECTED), 0.01);

            if (!FileUtil.exist(dataDir)) {
                FileUtil.mkdir(dataDir);
            }

            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            //目录为空无需加载ssTable
            if (files == null || files.length == 0) {
                lsmStore.walFile = new File(dataDir + WAL);
                lsmStore.wal = new RandomAccessFile(lsmStore.walFile, RW_MODE);
                return lsmStore;
            }
            //从大到小加载ssTable
            TreeMap<Long, SsTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (File file : files) {
                String fileName = file.getName();
                //从暂存的WAL中恢复数据，一般是持久化ssTable过程中异常才会留下walTmp
                if (file.isFile() && fileName.equals(WAL_TMP)) {
                    lsmStore.restoreFromWal(new RandomAccessFile(file, RW_MODE));
                }
                //加载ssTable
                if (file.isFile() && fileName.endsWith(TABLE)) {
                    int dotIndex = fileName.indexOf(".");
                    Long time = Long.parseLong(fileName.substring(0, dotIndex));
                    ssTableTreeMap.put(time, SsTable.restore(file.getAbsolutePath(), lsmStore.objectMapper));
                } else if (file.isFile() && fileName.equals(WAL)) {
                    //加载WAL
                    lsmStore.walFile = file;
                    lsmStore.wal = new RandomAccessFile(file, RW_MODE);
                    lsmStore.restoreFromWal(lsmStore.wal);
                }
            }

            lsmStore.ssTables.addAll(ssTableTreeMap.values());
            return lsmStore;
        } catch (FileNotFoundException t) {
            throw new EngineException(t.getMessage());
        }
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
                SsTable ssTable = SsTable.build(dataDir + System.currentTimeMillis() + TABLE, partSize, objectMapper);
                ssTable.write(merge);
                compact.addFirst(ssTable);
            }

            this.ssTables = compact;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void set(String key, String value) {
        bloomFilter.put(key);
        this.store(key, value, TableDataTypeEnum.SET);
    }

    public void remove(String key) {
        this.store(key, null, TableDataTypeEnum.REMOVE);
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
     * 保存
     *
     * @param key
     * @param value
     * @param tableDataTypeEnum
     */
    private void store(String key, String value, TableDataTypeEnum tableDataTypeEnum) {
        indexLock.writeLock().lock();
        try {
            TableData tableData = new TableData(key, value, tableDataTypeEnum);

            byte[] commandBytes = objectMapper.writeValueAsBytes(tableData);
            //先保存数据到WAL中
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);

            this.index.put(key, tableData);
            //内存表大小超过阈值进行持久化
            if (index.size() > storeThreshold) {
                switchIndex();
                storeToSsTable();
            }
        } catch (Exception e) {
            log.error("保存数据失败", e);
            throw new EngineException(e.getMessage());
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 从暂存日志中恢复数据
     *
     * @param wal
     */
    private void restoreFromWal(RandomAccessFile wal) {
        try {
            long len = wal.length();
            long start = 0;
            wal.seek(start);
            while (start < len) {
                //先读取数据大小
                int valueLen = wal.readInt();
                //根据数据大小读取数据
                byte[] bytes = new byte[valueLen];
                wal.read(bytes);

                TableData tableData = objectMapper.convertValue(new String(bytes, StandardCharsets.UTF_8), TableData.class);
                index.put(tableData.getKey(), tableData);

                start += 4;
                start += valueLen;
            }
            wal.seek(wal.length());
        } catch (Exception e) {
            log.error("恢复数据失败", e);
            throw new EngineException(e.getMessage());
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
            wal.close();
            //切换内存表后也要切换WAL
            File tmpWal = new File(dataDir + WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new EngineException("删除文件失败: walTmp");
                }
            }
            if (!walFile.renameTo(tmpWal)) {
                throw new EngineException("重命名文件失败: walTmp");
            }
            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile, RW_MODE);
        } catch (Exception e) {
            log.error("切换内存表失败", e);
            throw new EngineException(e.getMessage());
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 保存数据到ssTable
     */
    private void storeToSsTable() {
        try {
            //ssTable按照时间命名，这样可以保证名称递增
            SsTable ssTable = SsTable.build(dataDir + System.currentTimeMillis() + TABLE, partSize, objectMapper);
            ssTable.write(this.immutableIndex);
            this.ssTables.addFirst(ssTable);
            //持久化完成删除暂存的内存表和WAL_TMP
            this.immutableIndex = null;

            File tmpWal = new File(dataDir + WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new EngineException("删除文件失败: walTmp");
                }
            }
        } catch (Exception e) {
            log.error("保存到ss table失败", e);
            throw new EngineException(e.getMessage());
        }
    }
}
