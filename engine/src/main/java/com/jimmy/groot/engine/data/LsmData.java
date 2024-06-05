package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.data.lsm.LsmPartition;
import com.jimmy.groot.engine.exception.SqlException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Row;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.enums.ConditionEnum;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class LsmData extends AbstractData {

    private static final String KEY_ROW_SUFFIX = ":row:data";

    private long partSize;

    private String dataDir;

    private int expectCount;

    private String tableName;

    private int storeThreshold;

    private ObjectMapper objectMapper;

    private Map<String, Column> columnMap;

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
        lsmData.partitions = Maps.newConcurrentMap();
        lsmData.columnMap = columns.stream().collect(Collectors.toMap(Column::getName, g -> g));
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
            lsmPartition.set(uniqueDataKey + KEY_ROW_SUFFIX, objectMapper.writeValueAsString(row));

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                Column column = columnMap.get(key);
                if (column == null) {
                    throw new SqlException(key + "表字段不存在");
                }

                if (value == null) {
                    lsmPartition.set(uniqueDataKey + StrUtil.COLON + key, null);
                    continue;
                }

                Convert<?> convert = super.getConvert(column.getColumnType());
                lsmPartition.set(uniqueDataKey + StrUtil.COLON + key, convertValueToString(convert, value));
            }
        } catch (SqlException e) {
            throw e;
        } catch (Exception e) {
            throw new SqlException(e.getMessage());
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
            lsmPartition.remove(uniqueDataKey + KEY_ROW_SUFFIX);
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

    /**
     * 选取分区
     *
     * @param conditions
     * @return
     */
    private List<LsmPartition> selectPartition(List<Condition> conditions) {
        Map<String, Set<Object>> partitionData = Maps.newHashMap();
        Map<String, Condition> partitionConditions = Maps.newHashMap();

        for (Condition condition : conditions) {
            String fieldName = condition.getFieldName();
            ConditionEnum conditionEnum = condition.getConditionEnum();

            if (!partitionIndex.contain(fieldName)) {
                continue;
            }

            if (conditionEnum.equals(ConditionEnum.EQ) || conditionEnum.equals(ConditionEnum.IN)) {
                if (partitionConditions.put(fieldName, condition) != null) {
                    throw new SqlException("包含多个分区条件");
                }
            }
        }

        if (partitionConditions.size() != partitionIndex.size()) {
            throw new SqlException("条件需要未包含分区键");
        }

        for (Map.Entry<String, Condition> entry : partitionConditions.entrySet()) {
            String key = entry.getKey();
            Condition value = entry.getValue();
            Column column = columnMap.get(key);
            Object fieldValue = value.getFieldValue();
            ConditionEnum conditionEnum = value.getConditionEnum();

            Convert<?> convert = super.getConvert(column.getColumnType());

            switch (conditionEnum) {
                case EQ:
                    Object eqValue = convert.convert(fieldValue);
                    partitionData.put(key, Sets.newHashSet(eqValue));
                    break;
                case IN:
                    if (!(fieldValue instanceof Collection)) {
                        throw new IllegalArgumentException("in 操作需要使用集合类参数");
                    }

                    Collection<?> inCollection = (Collection<?>) fieldValue;
                    if (CollUtil.isEmpty(inCollection)) {
                        throw new IllegalArgumentException("in 集合为空");
                    }

                    Set<Object> inCollect = inCollection.stream().map(convert::convert).collect(Collectors.toSet());
                    partitionData.put(key, inCollect);
                    break;
            }
        }

        List<Map<String, Object>> maps = super.generateCombinations(partitionData, null);
        if (CollUtil.isEmpty(maps)) {
            throw new SqlException("选取分区失败");
        }

        List<LsmPartition> select = Lists.newArrayList();

        for (Map<String, Object> map : maps) {
            String key = super.getKey(map);

            LsmPartition lsmPartition = partitions.get(key);
            if (lsmPartition != null) {
                select.add(lsmPartition);
            }
        }

        return select;
    }

    /**
     * 转字符串
     *
     * @param convert
     * @param value
     * @param <T>
     * @return
     */
    private <T> String convertValueToString(Convert<T> convert, Object value) {
        if (value == null) {
            return null;
        }

        T convertedValue = (T) value;
        return convert.toString(convertedValue);
    }
}
