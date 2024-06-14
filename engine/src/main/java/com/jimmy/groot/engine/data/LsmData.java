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
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.element.ConditionElement;
import com.jimmy.groot.sql.element.QueryElement;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
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
        lsmData.uniqueStore = LsmStore.build(dataDir + StrUtil.SLASH + tableName + StrUtil.SLASH + lsmData.uniqueIndex.getName() + StrUtil.SLASH, storeThreshold, partSize, expectCount);
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
    public Collection<Map<String, Object>> query(QueryElement queryElement) {
        try {
            int end = queryElement.getEnd();
            int start = queryElement.getStart();
            Set<String> select = queryElement.getSelect();
            boolean isFindAll = queryElement.isSelectAll();
            boolean allColumn = queryElement.isAllColumn();
            boolean withoutCondition = queryElement.isWithoutCondition();
            Set<String> needColumnNames = queryElement.getNeedColumnNames();
            ConditionElement conditionElement = queryElement.getConditionElement();

            long sum = partitions.values().stream().mapToLong(LsmStore::total).sum();
            if (start >= sum) {
                return Lists.newArrayList();
            }
            //无条件查询
            if (withoutCondition) {
                if (MapUtil.isEmpty(partitions)) {
                    return Lists.newArrayList();
                }

                return this.withoutCondition(isFindAll, start, end, select, allColumn);
            }
            //根据条件查询
            List<Condition> conditions = conditionElement.getConditions();
            Collection<String> uniqueCodes = conditionElement.getUniqueCodes();
            Collection<String> partitionCodes = conditionElement.getPartitionCodes();
            if (CollUtil.isEmpty(partitionCodes)) {
                throw new SqlException("select partition fail");
            }

            int i = 0;
            List<Map<String, Object>> result = Lists.newArrayList();

            if (CollUtil.isNotEmpty(uniqueCodes)) {
                if (!isFindAll) {
                    for (String uniqueCode : uniqueCodes) {
                        String s = uniqueStore.get(uniqueCode + KEY_ROW_SUFFIX);
                        if (StrUtil.isEmpty(uniqueStore.get(uniqueCode + KEY_ROW_SUFFIX))) {
                            uniqueCodes.remove(s);
                        }
                    }

                    if (end > uniqueCodes.size()) {
                        return Lists.newArrayList();
                    }

                    uniqueCodes = CollUtil.sub(uniqueCodes, start, end);
                }

                for (String uniqueCode : uniqueCodes) {
                    Map<String, Object> data = this.uniqueCodeToData(uniqueCode, needColumnNames, allColumn);
                    if (data != null) {
                        if (CollUtil.isNotEmpty(conditions)) {
                            ConditionExpression conditionExp = this.getConditionExp(conditions);

                            if (this.filter(data, conditionExp.getConditionArgument(), conditionExp.getExpression())) {
                                result.add(super.columnFilter(data, select));
                            }
                        } else {
                            result.add(super.columnFilter(data, select));
                        }
                    }
                }
            } else {
                Collection<Map<String, Object>> maps = this.withoutCondition(isFindAll, start, end, select, allColumn);

                if (CollUtil.isNotEmpty(maps)) {
                    for (Map<String, Object> map : maps) {
                        if (CollUtil.isNotEmpty(conditions)) {
                            ConditionExpression conditionExp = this.getConditionExp(conditions);

                            if (this.filter(map, conditionExp.getConditionArgument(), conditionExp.getExpression())) {
                                result.add(super.columnFilter(map, select));
                            }
                        } else {
                            result.add(super.columnFilter(map, select));
                        }
                    }
                }
            }

            return isFindAll ? result : CollUtil.sub(result, start, end);
        } catch (Exception e) {
            log.error("查询失败", e);
            throw new SqlException("select fail:" + e.getMessage());
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
    private Collection<Map<String, Object>> withoutCondition(boolean isFindAll, int start, int end, Set<String> needColumnNames, boolean isAllColumn) throws Exception {
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
