package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.data.lsm.LsmStore;
import com.jimmy.groot.engine.data.other.ConditionPart;
import com.jimmy.groot.engine.exception.SqlException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Index;
import com.jimmy.groot.engine.metadata.Row;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.enums.ConditionEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class LsmData extends AbstractData {

    private static final String KEY_ROW_SUFFIX = ":row:data";

    private long partSize;

    private String dataDir;

    private int expectCount;

    private String tableName;

    private int storeThreshold;

    private LsmStore uniqueStore;

    private ObjectMapper objectMapper;

    private Map<String, Column> columnMap;

    private ConcurrentMap<String, LsmStore> partitions;

    private LsmData(List<Column> columns) {
        super(columns);
    }

    public static LsmData build(List<Column> columns, String dataDir, String tableName, int storeThreshold, int partSize, int expectCount) {
        LsmData lsmData = new LsmData(columns);
        lsmData.dataDir = dataDir;
        lsmData.partSize = partSize;
        lsmData.tableName = tableName;
        lsmData.expectCount = expectCount;
        lsmData.storeThreshold = storeThreshold;
        lsmData.objectMapper = new ObjectMapper();
        lsmData.partitions = Maps.newConcurrentMap();
        lsmData.columnMap = columns.stream().collect(Collectors.toMap(Column::getName, g -> g));
        lsmData.uniqueStore = LsmStore.build(dataDir + StrUtil.SLASH + tableName + StrUtil.SLASH + lsmData.uniqueIndex.getName() + StrUtil.SLASH, storeThreshold, partSize, expectCount)
        return lsmData;
    }

    @Override
    public void save(Map<String, Object> doc) {
        try {
            IndexData uniqueData = super.getUniqueData(doc);
            IndexData partitionData = super.getPartitionData(doc);

            String uniqueDataKey = uniqueData.getKey();
            String partitionDataKey = partitionData.getKey();

            String path = dataDir + StrUtil.SLASH + tableName + StrUtil.SLASH + partitionDataKey + StrUtil.SLASH;
            partitions.computeIfAbsent(partitionDataKey, s -> LsmStore.build(path, storeThreshold, partSize, expectCount));

            LsmStore lsmStore = partitions.get(partitionDataKey);

            Row row = new Row();
            row.setUniqueDataKey(uniqueDataKey);
            row.setPartitionDataKey(partitionDataKey);
            row.setInsertTime(System.currentTimeMillis());
            uniqueStore.set(uniqueDataKey + KEY_ROW_SUFFIX, objectMapper.writeValueAsString(row));

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                Column column = columnMap.get(key);
                if (column == null) {
                    throw new SqlException(key + "表字段不存在");
                }

                if (value == null) {
                    lsmStore.set(uniqueDataKey + StrUtil.COLON + key, null);
                    continue;
                }

                Convert<?> convert = super.getConvert(column.getColumnType());
                lsmStore.set(uniqueDataKey + StrUtil.COLON + key, convertValueToString(convert, value));
            }
        } catch (SqlException e) {
            throw e;
        } catch (Exception e) {
            throw new SqlException(e.getMessage());
        }
    }

    @Override
    public void remove(Map<String, Object> doc) {
        uniqueStore.remove(super.getUniqueData(doc).getKey() + KEY_ROW_SUFFIX);
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
     * 查询数据
     *
     * @param queryPlus
     * @param start
     * @param end
     * @return
     */
    private Collection<Map<String, Object>> query(QueryPlus queryPlus, int start, int end) {
        long sum = partitions.values().stream().mapToLong(LsmStore::total).sum();
        if (start >= sum) {
            return Lists.newArrayList();
        }

        boolean isFindAll = start < 0 && end < 0;

        List<List<Condition>> conditionGroups = queryPlus.getConditionGroups();
        if (CollUtil.isEmpty(conditionGroups)) {
            if (MapUtil.isEmpty(partitions)) {
                return Lists.newArrayList();
            }

            return this.withoutCondition(isFindAll, start, end);
        }


        
    }

    /**
     * 不包含条件查询
     *
     * @return
     */
    private Collection<Map<String, Object>> withoutCondition(boolean isFindAll, int start, int end) {
        try {
            int i = 0;
            List<Map<String, Object>> result = Lists.newArrayList();

            TreeMap<String, String> all = uniqueStore.all();
            if (MapUtil.isEmpty(all)) {
                return Lists.newArrayList();
            }

            Collection<String> keySet = all.keySet();
            if (!isFindAll) {
                keySet = CollUtil.sub(keySet, start, end);
            }

            for (String s : keySet) {
                String uniqueDataKey = StrUtil.removeAll(s, KEY_ROW_SUFFIX);

                Row row = objectMapper.readValue(all.get(s), Row.class);
                String partitionDataKey = row.getPartitionDataKey();

                LsmStore lsmStore = partitions.get(partitionDataKey);
                if (lsmStore == null) {
                    continue;
                }

                Map<String, Object> data = Maps.newHashMap();

                for (Column column : columns) {
                    String name = column.getName();

                    String s = lsmStore.get(uniqueDataKey + StrUtil.COLON + name);
                    data.put(name, s == null ? null : super.getConvert(column.getColumnType()).convert(s));
                }

                i++;
                result.add(data);

                if (!isFindAll && i == end) {
                    return result;
                }
            }

            return result;
        } catch (Exception e) {
            log.error("查询失败", e);
            throw new SqlException(" select fail");
        }
    }


    /**
     * 判断是否只含有分区条件
     *
     * @param conditions
     * @return
     */
    private boolean isOnlyPartitionCondition(List<Condition> conditions) {
        if (conditions.size() > 1) {
            return false;
        }

        Condition condition = conditions.stream().findFirst().get();
        ConditionEnum conditionEnum = condition.getConditionEnum();
        return conditionEnum.equals(ConditionEnum.IN) || conditionEnum.equals(ConditionEnum.EQ);
    }


    /**
     * 条件分析
     *
     * @param conditions
     * @return
     */
    private ConditionPart analysisCondition(List<Condition> conditions) {
        List<Map<String, Object>> partitionData = this.collectCondition(conditions, super.partitionIndex);
        if (CollUtil.isEmpty(partitionData)) {
            throw new SqlException("select partition fail");
        }

        List<Map<String, Object>> uniqueData = this.collectCondition(conditions, super.uniqueIndex);
        if (CollUtil.isEmpty(uniqueData)) {
            throw new SqlException("unique data is null");
        }

        ConditionPart conditionPart = new ConditionPart();
        for (Map<String, Object> partitionDatum : partitionData) {
            conditionPart.getPartitionCodes().add(super.getKey(partitionDatum));
        }

        for (Map<String, Object> uniqueDatum : uniqueData) {
            conditionPart.getUniqueCodes().add(super.getKey(uniqueDatum));
        }

        return conditionPart;
    }


    /**
     * 收集条件
     *
     * @param conditions
     * @return
     */
    private List<Map<String, Object>> collectCondition(List<Condition> conditions, Index index) {
        Map<String, Set<Object>> data = Maps.newHashMap();
        Map<String, Condition> conditionMap = Maps.newHashMap();

        for (Condition condition : conditions) {
            String fieldName = condition.getFieldName();
            ConditionEnum conditionEnum = condition.getConditionEnum();

            if (index.contain(fieldName)) {
                if (conditionEnum.equals(ConditionEnum.EQ) || conditionEnum.equals(ConditionEnum.IN)) {
                    if (conditionMap.put(fieldName, condition) != null) {
                        throw new SqlException("contain many " + index.getName() + " columns");
                    }
                }
            }
        }

        if (conditionMap.size() != index.size()) {
            throw new SqlException("must contain " + index.getName() + " columns");
        }

        for (Map.Entry<String, Condition> entry : conditionMap.entrySet()) {
            String key = entry.getKey();
            Condition value = entry.getValue();
            Column column = columnMap.get(key);
            Object fieldValue = value.getFieldValue();
            ConditionEnum conditionEnum = value.getConditionEnum();

            Convert<?> convert = super.getConvert(column.getColumnType());

            switch (conditionEnum) {
                case EQ:
                    Object eqValue = convert.convert(fieldValue);
                    data.put(key, Sets.newHashSet(eqValue));
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
                    data.put(key, inCollect);
                    break;
            }
        }

        return super.generateCombinations(data, null);
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
