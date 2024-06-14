package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.data.lsm.LsmStore;
import com.jimmy.groot.engine.data.other.ConditionExpression;
import com.jimmy.groot.engine.data.other.IndexData;
import com.jimmy.groot.engine.exception.SqlException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Row;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.element.ConditionElement;
import com.jimmy.groot.sql.element.QueryElement;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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
        lsmData.uniqueStore = LsmStore.build(dataDir + StrUtil.SLASH + tableName + StrUtil.SLASH + lsmData.uniqueIndex.getName() + StrUtil.SLASH, storeThreshold, partSize, expectCount);
        return lsmData;
    }

    @Override
    public void save(Map<String, Object> doc) {
        try {
            IndexData uniqueData = super.getIndexData(doc, super.uniqueIndex);
            IndexData partitionData = super.getIndexData(doc, super.partitionIndex);

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

                Column column = super.columnMap.get(key);
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
        uniqueStore.remove(super.getIndexData(doc, super.uniqueIndex).getKey() + KEY_ROW_SUFFIX);
    }

    @Override
    protected Collection<Map<String, Object>> queryList(QueryElement queryElement) throws Exception {
        int end = queryElement.getEnd();
        int start = queryElement.getStart();
        boolean isFindAll = queryElement.isSelectAll();
        boolean allColumn = queryElement.isAllColumn();
        boolean withoutCondition = queryElement.isWithoutCondition();
        Set<String> needColumnNames = queryElement.getNeedColumnNames();
        List<ConditionElement> conditionElements = queryElement.getConditionElements();

        long sum = partitions.values().stream().mapToLong(LsmStore::total).sum();
        if (start > sum) {
            return Lists.newArrayList();
        }
        //无条件查询
        if (withoutCondition || CollUtil.isEmpty(conditionElements)) {
            if (MapUtil.isEmpty(partitions)) {
                return Lists.newArrayList();
            }

            return this.withoutCondition(isFindAll, start, end, needColumnNames, allColumn);
        }

        AtomicInteger total = new AtomicInteger(0);
        Map<String, Map<String, Object>> records = Maps.newHashMap();
        Collection<Map<String, Object>> allData = Lists.newArrayList();

        for (ConditionElement conditionElement : conditionElements) {
            Set<String> uniqueCodes = conditionElement.getUniqueCodes();

            if (CollUtil.isNotEmpty(uniqueCodes)) {
                this.queryByUnique(conditionElement,
                        records,
                        isFindAll,
                        start,
                        end,
                        total,
                        allColumn,
                        needColumnNames);
            } else {
                if (CollUtil.isEmpty(allData)) {
                    allData.addAll(this.withoutCondition(true,
                            start,
                            end,
                            needColumnNames,
                            allColumn));
                }

                if (CollUtil.isEmpty(allData)) {
                    return Lists.newArrayList();
                }

                this.queryAll(conditionElement,
                        records,
                        isFindAll,
                        start,
                        end,
                        total,
                        allData,
                        needColumnNames,
                        allColumn);
            }

            if (!isFindAll && total.get() >= end) {
                break;
            }
        }

        return isFindAll ? records.values() : CollUtil.sub(records.values(), 0, end - start);
    }

    @Override
    protected Map<String, Object> uniqueKeyToData(String partitionKey, String uniqueKey, Set<String> needColumnNames, boolean isAllColumn) throws Exception {
        Row row = objectMapper.readValue(uniqueStore.get(uniqueKey), Row.class);
        String partitionDataKey = row.getPartitionDataKey();

        LsmStore lsmStore = partitions.get(partitionDataKey);
        if (lsmStore == null) {
            return null;
        }

        Map<String, Object> data = Maps.newHashMap();

        for (Column column : super.columnMap.values()) {
            String name = column.getName();

            if (!isAllColumn && !needColumnNames.contains(name)) {
                continue;
            }

            String value = lsmStore.get(uniqueKey + StrUtil.COLON + name);
            data.put(name, value == null ? null : super.getConvert(column.getColumnType()).convert(value));
        }

        return data;
    }

    /**
     * 查询所有
     *
     * @param conditionElement
     * @param records
     * @param isFindAll
     * @param end
     * @param total
     * @param allData
     */
    private void queryAll(ConditionElement conditionElement,
                          Map<String, Map<String, Object>> records,
                          boolean isFindAll,
                          int start,
                          int end,
                          AtomicInteger total,
                          Collection<Map<String, Object>> allData,
                          Set<String> needColumnNames,
                          boolean isAllColumn) throws Exception {

        List<Condition> conditions = conditionElement.getConditions();
        //获取表达式
        ConditionExpression conditionExpression = this.getConditionExpression(conditions);

        for (Map<String, Object> data : allData) {
            if (!isFindAll && total.get() > end) {
                break;
            }

            IndexData indexData = super.getIndexData(data, super.uniqueIndex);

            String uniqueKey = indexData.getKey();

            if (records.containsKey(uniqueKey)) {
                continue;
            }

            this.filterAndPut(conditionExpression,
                    uniqueKey,
                    null,
                    total,
                    records,
                    isFindAll ? 0 : start,
                    needColumnNames,
                    isAllColumn);
        }
    }

    /**
     * 根据主键查询
     *
     * @param conditionElement
     * @param records
     * @param isFindAll
     * @param end
     * @param total
     * @param allColumn
     * @param needColumnNames
     * @throws Exception
     */
    private void queryByUnique(ConditionElement conditionElement,
                               Map<String, Map<String, Object>> records,
                               boolean isFindAll,
                               int start,
                               int end,
                               AtomicInteger total,
                               boolean allColumn,
                               Set<String> needColumnNames) throws Exception {
        List<Condition> conditions = conditionElement.getConditions();
        Collection<String> uniqueCodes = conditionElement.getUniqueCodes();
        Collection<String> partitionCodes = conditionElement.getPartitionCodes();
        if (CollUtil.isEmpty(partitionCodes)) {
            throw new SqlException("select partition fail");
        }
        //获取表达式
        ConditionExpression conditionExpression = this.getConditionExpression(conditions);

        if (!isFindAll) {
            uniqueCodes.removeIf(uniqueCode -> StrUtil.isEmpty(uniqueStore.get(uniqueCode + KEY_ROW_SUFFIX)) || records.containsKey(uniqueCode));

            if (CollUtil.isEmpty(uniqueCodes)) {
                return;
            }

            int andAdd = total.getAndAdd(uniqueCodes.size());
            uniqueCodes = CollUtil.sub(uniqueCodes, andAdd, end);
        }

        for (String uniqueCode : uniqueCodes) {
            this.filterAndPut(conditionExpression,
                    uniqueCode,
                    null,
                    total,
                    records,
                    isFindAll ? 0 : start,
                    needColumnNames,
                    allColumn);
        }
    }

    /**
     * 获取条件表达式
     *
     * @param conditions
     * @return
     */
    private ConditionExpression getConditionExpression(List<Condition> conditions) {
        int i = 0;
        StringBuilder otherExpression = new StringBuilder();
        StringBuilder uniqueExpression = new StringBuilder();
        ConditionExpression conditionExpression = new ConditionExpression();

        if (CollUtil.isEmpty(conditions)) {
            return conditionExpression;
        }

        for (Condition condition : conditions) {
            Column column = super.columnMap.get(condition.getFieldName());

            Boolean isUniqueKey = column.getIsUniqueKey();

            String expCondition = super.getExpCondition(
                    column,
                    condition.getFieldValue(),
                    condition.getConditionEnum(),
                    isUniqueKey ? conditionExpression.getUniqueConditionArgument() : conditionExpression.getOtherConditionArgument(),
                    i++);

            StringBuilder expression = isUniqueKey ? uniqueExpression : otherExpression;

            if (StrUtil.isNotBlank(expression)) {
                expression.append(ConditionTypeEnum.AND.getExpression());
            }

            expression.append(expCondition);
        }

        if (StrUtil.isNotBlank(otherExpression)) {
            conditionExpression.setOtherExpression(AviatorEvaluator.compile(otherExpression.toString()));
        }

        if (StrUtil.isNotBlank(uniqueExpression)) {
            conditionExpression.setUniqueExpression(AviatorEvaluator.compile(uniqueExpression.toString()));
        }

        return conditionExpression;
    }

    /**
     * 不包含条件查询
     *
     * @return
     */
    private Collection<Map<String, Object>> withoutCondition(boolean isFindAll,
                                                             int start,
                                                             int end,
                                                             Set<String> needColumnNames,
                                                             boolean isAllColumn) throws Exception {
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

            Map<String, Object> data = this.uniqueKeyToData(null, uniqueDataKey, needColumnNames, isAllColumn);
            if (data == null) {
                continue;
            }

            i++;
            result.add(data);

            if (!isFindAll && i == end) {
                return result;
            }
        }

        return result;
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
