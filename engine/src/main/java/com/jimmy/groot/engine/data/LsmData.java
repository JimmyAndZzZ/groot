package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.data.lsm.LsmStore;
import com.jimmy.groot.engine.data.other.ConditionExpression;
import com.jimmy.groot.engine.data.other.ConditionPart;
import com.jimmy.groot.engine.exception.SqlException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Index;
import com.jimmy.groot.engine.metadata.Row;
import com.jimmy.groot.sql.core.AggregateFunction;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.element.ConditionElement;
import com.jimmy.groot.sql.element.QueryElement;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.jimmy.groot.platform.constant.ClientConstant.SOURCE_PARAM_KEY;
import static com.jimmy.groot.platform.constant.ClientConstant.TARGET_PARAM_KEY;

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
        uniqueStore.remove(super.getIndexData(doc, super.uniqueIndex).getKey() + KEY_ROW_SUFFIX);
    }

    @Override
    public Collection<Map<String, Object>> query(QueryElement queryElement) {
        try {
            Collection<Map<String, Object>> maps = this.queryList(queryElement);
            if (CollUtil.isEmpty(maps)) {
                return Lists.newArrayList();
            }

            Set<String> select = queryElement.getSelect();
            List<AggregateFunction> aggregateFunctions = queryElement.getAggregateFunctions();

            if (CollUtil.isNotEmpty(aggregateFunctions)) {
                return super.aggregateHandler(select, aggregateFunctions, maps);
            }

            if (CollUtil.isEmpty(select)) {
                return maps;
            }

            return maps.stream().map(map -> super.columnFilter(map, select)).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("查询失败", e);
            throw new SqlException("select fail:" + e.getMessage());
        }
    }

    /**
     * 查询数据
     *
     * @param queryElement
     * @return
     */
    private Collection<Map<String, Object>> queryList(QueryElement queryElement) throws Exception {
        int end = queryElement.getEnd();
        int start = queryElement.getStart();
        boolean isFindAll = queryElement.isSelectAll();
        boolean allColumn = queryElement.isAllColumn();
        boolean withoutCondition = queryElement.isWithoutCondition();
        Set<String> needColumnNames = queryElement.getNeedColumnNames();
        List<ConditionElement> conditionElements = queryElement.getConditionElements();

        long sum = partitions.values().stream().mapToLong(LsmStore::total).sum();
        if (start >= sum) {
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
                        allData);
            }

            if (total.get() >= end) {
                break;
            }
        }

        return CollUtil.sub(records.values(), start, end);
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
                          Collection<Map<String, Object>> allData) {

        List<Condition> conditions = conditionElement.getConditions();

        for (Map<String, Object> data : allData) {
            if (!isFindAll && total.get() > end) {
                break;
            }

            IndexData indexData = super.getIndexData(data, super.uniqueIndex);

            if (records.containsKey(indexData.getKey())) {
                continue;
            }

            this.put(conditions, data, total, records, start);
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
        //根据条件查询
        List<Condition> conditions = conditionElement.getConditions();
        Collection<String> uniqueCodes = conditionElement.getUniqueCodes();
        Collection<String> partitionCodes = conditionElement.getPartitionCodes();
        if (CollUtil.isEmpty(partitionCodes)) {
            throw new SqlException("select partition fail");
        }

        if (!isFindAll) {
            for (String uniqueCode : uniqueCodes) {
                String s = uniqueStore.get(uniqueCode + KEY_ROW_SUFFIX);
                if (StrUtil.isEmpty(uniqueStore.get(uniqueCode + KEY_ROW_SUFFIX)) || records.containsKey(uniqueCode)) {
                    uniqueCodes.remove(s);
                }
            }

            if (CollUtil.isEmpty(uniqueCodes)) {
                return;
            }

            int andAdd = total.getAndAdd(uniqueCodes.size());
            uniqueCodes = CollUtil.sub(uniqueCodes, andAdd, end);
        }

        for (String uniqueCode : uniqueCodes) {
            Map<String, Object> data = this.uniqueCodeToData(uniqueCode, needColumnNames, allColumn);
            if (data != null) {
                this.put(conditions, data, total, records, start);
            }
        }
    }

    /**
     * 填充数据
     *
     * @param conditions
     * @param data
     * @param total
     * @param records
     * @param start
     */
    private void put(List<Condition> conditions,
                     Map<String, Object> data,
                     AtomicInteger total,
                     Map<String, Map<String, Object>> records,
                     int start) {
        if (CollUtil.isNotEmpty(conditions)) {
            ConditionExpression conditionExp = this.getConditionExp(conditions);

            if (this.filter(data, conditionExp.getConditionArgument(), conditionExp.getExpression())) {
                int i = total.incrementAndGet();
                if (i > start) {
                    super.putRecords(records, data);
                }
            }
        } else {
            int i = total.incrementAndGet();
            if (i > start) {
                super.putRecords(records, data);
            }
        }
    }


    /**
     * 过滤数据
     *
     * @param d
     * @param conditionArgument
     * @param expression
     * @return
     */
    private boolean filter(Map<String, Object> d, Map<String, Object> conditionArgument, Expression expression) {
        Map<String, Object> param = Maps.newHashMap();
        param.put(SOURCE_PARAM_KEY, d);
        param.put(TARGET_PARAM_KEY, conditionArgument);
        return cn.hutool.core.convert.Convert.toBool(expression.execute(param), false);
    }

    /**
     * 获取条件表达式
     *
     * @param conditions
     * @return
     */
    private ConditionExpression getConditionExp(List<Condition> conditions) {
        int i = 0;
        StringBuilder expression = new StringBuilder();
        ConditionExpression conditionExpression = new ConditionExpression();

        for (Condition condition : conditions) {
            String fieldName = condition.getFieldName();

            Column column = columnMap.get(fieldName);

            String expCondition = super.getExpCondition(column, conditionExpression.getKeyConditionValue(), condition.getFieldValue(), condition.getConditionEnum(), conditionExpression.getConditionArgument(), i++);

            if (StrUtil.isNotBlank(expression)) {
                expression.append(ConditionTypeEnum.AND.getExpression());
            }

            expression.append(expCondition);
        }

        conditionExpression.setExpression(AviatorEvaluator.compile(expression.toString()));
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

            Map<String, Object> data = this.uniqueCodeToData(uniqueDataKey, needColumnNames, isAllColumn);
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
     * 主键转数据
     *
     * @return
     */
    private Map<String, Object> uniqueCodeToData(String uniqueCode, Set<String> needColumnNames, boolean isAllColumn) throws JsonProcessingException {
        Row row = objectMapper.readValue(uniqueStore.get(uniqueCode), Row.class);
        String partitionDataKey = row.getPartitionDataKey();

        LsmStore lsmStore = partitions.get(partitionDataKey);
        if (lsmStore == null) {
            return null;
        }

        Map<String, Object> data = Maps.newHashMap();

        for (Column column : columns) {
            String name = column.getName();

            if (!isAllColumn && !needColumnNames.contains(name)) {
                continue;
            }

            String value = lsmStore.get(uniqueCode + StrUtil.COLON + name);
            data.put(name, value == null ? null : super.getConvert(column.getColumnType()).convert(value));
        }

        return data;
    }


    /**
     * 收集主键条件
     *
     * @param conditions
     * @return
     */
    private Set<String> collectUniqueIndexCondition(List<Condition> conditions) {
        Map<String, Set<Object>> data = Maps.newHashMap();

        for (int i = conditions.size() - 1; i >= 0; i--) {
            Condition condition = conditions.get(i);

            String fieldName = condition.getFieldName();
            Object fieldValue = condition.getFieldValue();
            ConditionEnum conditionEnum = condition.getConditionEnum();

            if (super.uniqueIndex.contain(fieldName)) {
                if (conditionEnum.equals(ConditionEnum.EQ) || conditionEnum.equals(ConditionEnum.IN)) {
                    conditions.remove(i);

                    Convert<?> convert = super.getConvert(columnMap.get(fieldName).getColumnType());

                    switch (conditionEnum) {
                        case EQ:
                            Object eqValue = convert.convert(fieldValue);
                            data.put(fieldName, Sets.newHashSet(eqValue));
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
                            data.put(fieldName, inCollect);
                            break;
                    }
                }
            }
        }

        return super.generateCombinations(data, null).stream().map(super::getKey).collect(Collectors.toSet());
    }

    /**
     * 收集分区条件
     *
     * @param conditions
     * @return
     */
    private Set<String> collectPartitionIndexCondition(List<Condition> conditions) {
        Map<String, Set<Object>> data = Maps.newHashMap();
        Map<String, Condition> conditionMap = Maps.newHashMap();

        for (int i = conditions.size() - 1; i >= 0; i--) {
            Condition condition = conditions.get(i);

            String fieldName = condition.getFieldName();
            ConditionEnum conditionEnum = condition.getConditionEnum();

            if (super.partitionIndex.contain(fieldName)) {
                if (conditionEnum.equals(ConditionEnum.EQ) || conditionEnum.equals(ConditionEnum.IN)) {
                    if (conditionMap.put(fieldName, condition) != null) {
                        throw new SqlException("contain many partitionIndex  columns");
                    }
                }
            }
        }

        if (conditionMap.size() != super.partitionIndex.size()) {
            throw new SqlException("must contain partitionIndex columns");
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

        return super.generateCombinations(data, null).stream().map(super::getKey).collect(Collectors.toSet());
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
