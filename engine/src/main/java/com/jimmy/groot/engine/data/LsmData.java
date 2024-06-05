package com.jimmy.groot.engine.data;

import cn.hutool.core.io.FileUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.jimmy.groot.engine.data.lsm.LsmPartition;
import com.jimmy.groot.engine.data.memory.MemoryFragment;
import com.jimmy.groot.engine.data.memory.MemoryPartition;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Row;
import com.jimmy.groot.sql.core.QueryPlus;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmData extends AbstractData {

    private long partSize;

    private String dataDir;

    private int expectCount;

    private String tableName;

    private int storeThreshold;

    private ObjectMapper objectMapper;

    private ConcurrentMap<String, LsmPartition> partitions;

    private LsmData(List<Column> columns) {
        super(columns);
    }

    public static LsmData build(List<Column> columns,
                                String dataDir,
                                String tableName,
                                int storeThreshold,
                                int partSize,
                                int expectCount) {
        LsmData lsmData = new LsmData(columns);
        lsmData.dataDir = dataDir;
        lsmData.partSize = partSize;
        lsmData.tableName = tableName;
        lsmData.expectCount = expectCount;
        lsmData.storeThreshold = storeThreshold;
        lsmData.objectMapper = new ObjectMapper();
        return lsmData;
    }

    @Override
    public void save(Map<String, Object> doc) {
        try {
            IndexData uniqueData = super.getUniqueData(doc);
            IndexData partitionData = super.getPartitionData(doc);

            String uniqueDataKey = uniqueData.getKey();
            String partitionDataKey = partitionData.getKey();

            partitions.computeIfAbsent(partitionDataKey, s -> LsmPartition.build(dataDir,
                    tableName,
                    storeThreshold,
                    partSize,
                    expectCount));

            LsmPartition lsmPartition = partitions.get(partitionDataKey);

            Row row = new Row();
            row.setUniqueDataKey(uniqueDataKey);
            row.setPartitionDataKey(partitionDataKey);
            row.setInsertTime(System.currentTimeMillis());
            lsmPartition.set(uniqueDataKey + ":row:data", objectMapper.writeValueAsString(row));
        } catch (Exception e) {
            throw new EngineException(e.getMessage());
        }
    }

    @Override
    public void remove(Map<String, Object> doc) {
        IndexData uniqueData = super.getUniqueData(doc);
        IndexData partitionData = super.getPartitionData(doc);

        String uniqueDataKey = uniqueData.getKey();
        String partitionDataKey = partitionData.getKey();

        LsmPartition lsmPartition = partitions.get(partitionDataKey);
        if (lsmPartition != null) {
            lsmPartition.remove(uniqueDataKey + ":row:data");
        }
    }

    @Override
    public Collection<Map<String, Object>> page(QueryPlus queryPlus, Integer pageNo, Integer pageSize) {
        return null;
    }

    @Override
    public Collection<Map<String, Object>> list(QueryPlus queryPlus) {
        return null;
    }
}
