package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.base.Data;
import com.jimmy.groot.engine.convert.DateConvert;
import com.jimmy.groot.engine.convert.DefaultConvert;
import com.jimmy.groot.engine.data.other.IndexData;
import com.jimmy.groot.engine.exception.SqlException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Index;
import com.jimmy.groot.platform.other.Assert;
import com.jimmy.groot.sql.core.AggregateEnum;
import com.jimmy.groot.sql.core.AggregateFunction;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.element.QueryElement;
import com.jimmy.groot.sql.enums.ColumnTypeEnum;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.other.MapComparator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.jimmy.groot.platform.constant.ClientConstant.SOURCE_PARAM_KEY;
import static com.jimmy.groot.platform.constant.ClientConstant.TARGET_PARAM_KEY;

@Slf4j
public abstract class AbstractData implements Data {

    private final Map<ColumnTypeEnum, Convert<?>> converts = Maps.newHashMap();

    protected Index uniqueIndex;

    protected Index partitionIndex;

    protected List<Column> columns;

    public AbstractData(List<Column> columns) {
        this.columns = columns;
        this.converts.put(ColumnTypeEnum.DATE, new DateConvert());

        this.columns = columns;
        this.uniqueIndex = new Index("unique key");
        this.partitionIndex = new Index("partition key");

        for (Column column : columns) {
            String name = column.getName();

            if (column.getIsPartitionKey()) {
                partitionIndex.addColumn(name);
            }

            if (column.getIsUniqueKey()) {
                uniqueIndex.addColumn(name);
            }
        }

        Assert.isTrue(!partitionIndex.isEmpty(), "分区键为空");
        Assert.isTrue(!uniqueIndex.isEmpty(), "唯一键为空");
    }

    protected abstract Collection<Map<String, Object>> queryList(QueryElement queryElement) throws Exception;

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
                return this.aggregateHandler(select, aggregateFunctions, maps);
            }

            if (CollUtil.isEmpty(select)) {
                return maps;
            }

            return maps.stream().map(map -> this.columnFilter(map, select)).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("查询失败", e);
            throw new SqlException("select fail:" + e.getMessage());
        }
    }

    /**
     * 聚合函数处理
     *
     * @param result
     * @return
     */
    protected Collection<Map<String, Object>> aggregateHandler(Set<String> select, List<AggregateFunction> aggregateFunctions, Collection<Map<String, Object>> result) {
        if (CollUtil.isEmpty(result)) {
            return result;
        }

        if (CollUtil.isEmpty(aggregateFunctions)) {
            return result;
        }

        Map<String, Object> doc = Maps.newHashMap();

        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            doc.put(aggregateFunction.getAlias(), this.aggregateCalculate(result, aggregateFunction.getAggregateType(), aggregateFunction.getColumn()));
        }

        if (CollUtil.isEmpty(select)) {
            return Lists.newArrayList(doc);
        }

        return result.stream().map(map -> {
            Map<String, Object> data = Maps.newHashMap(doc);
            for (String s : select) {
                data.put(s, map.get(s));
            }

            return data;
        }).collect(Collectors.toList());
    }

    /**
     * 填充数据
     *
     * @param records
     * @param data
     */
    protected void putRecords(Map<String, Map<String, Object>> records, Map<String, Object> data) {
        IndexData indexData = this.getIndexData(data, this.uniqueIndex);
        records.put(indexData.getKey(), data);
    }

    /**
     * select 字段过滤
     *
     * @param data
     * @param select
     * @return
     */
    protected Map<String, Object> columnFilter(Map<String, Object> data, Set<String> select) {
        return CollUtil.isEmpty(select) ? data : select.stream().collect(Collectors.toMap(s -> s, data::get));
    }

    /**
     * 构建表达式
     *
     * @param column
     * @param keyConditionValue
     * @param fieldValue
     * @param conditionEnum
     * @param target
     * @param i
     * @return
     */
    protected String getExpCondition(Column column, Map<String, Set<Object>> keyConditionValue, Object fieldValue, ConditionEnum conditionEnum, Map<String, Object> target, int i) {
        String name = column.getName();
        String keyName = name + "$" + i;
        StringBuilder conditionExp = new StringBuilder();
        Convert<?> convert = this.getConvert(column.getColumnType());
        conditionExp.append(SOURCE_PARAM_KEY).append(".").append(name);

        if (column.getIsUniqueKey() || column.getIsPartitionKey()) {
            keyConditionValue.computeIfAbsent(name, s -> new HashSet<>());
        }

        switch (conditionEnum) {
            case EQ:
                Object eqValue = convert.convert(fieldValue);

                conditionExp.append("==").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, eqValue);

                if (column.getIsUniqueKey() || column.getIsPartitionKey()) {
                    keyConditionValue.get(name).add(eqValue);
                }

                break;
            case GT:
                conditionExp.append("> ").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, convert.convert(fieldValue));
                break;
            case GE:
                conditionExp.append(">=").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, convert.convert(fieldValue));
                break;
            case LE:
                conditionExp.append("<=").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, convert.convert(fieldValue));
                break;
            case LT:
                conditionExp.append("< ").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, convert.convert(fieldValue));
                break;
            case IN:
                conditionExp.setLength(0);
                conditionExp.append(" in (").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("in 操作需要使用集合类参数");
                }

                Collection<?> inCollection = (Collection<?>) fieldValue;
                if (CollUtil.isEmpty(inCollection)) {
                    throw new IllegalArgumentException("in 集合为空");
                }

                List<?> inCollect = inCollection.stream().map(convert::convert).collect(Collectors.toList());
                target.put(keyName, inCollect);

                if (column.getIsUniqueKey() || column.getIsPartitionKey()) {
                    keyConditionValue.get(name).addAll(inCollect);
                }

                target.put(keyName, fieldValue);
                break;
            case NOT_IN:
                conditionExp.setLength(0);
                conditionExp.append(" notIn (").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                if (!(fieldValue instanceof Collection)) {
                    throw new IllegalArgumentException("not in 操作需要使用集合类参数");
                }

                Collection<?> notInCollection = (Collection<?>) fieldValue;
                if (CollUtil.isEmpty(notInCollection)) {
                    throw new IllegalArgumentException("not in 集合为空");
                }

                List<?> notInCollect = notInCollection.stream().map(convert::convert).collect(Collectors.toList());
                target.put(keyName, notInCollect);
                break;
            case NULL:
                conditionExp.append("==nil");
                break;
            case NOT_NULL:
                conditionExp.append("!=nil");
                break;
            case NE:
                conditionExp.append("!=").append(TARGET_PARAM_KEY).append(".").append(keyName);
                target.put(keyName, convert.convert(fieldValue));
                break;
            case NOT_LIKE:
                conditionExp.setLength(0);
                conditionExp.append("!string.contains(").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, this.likeValueHandler(fieldValue));
                break;
            case LIKE:
                conditionExp.setLength(0);
                conditionExp.append("string.contains(").append(SOURCE_PARAM_KEY).append(".").append(name).append(",").append(TARGET_PARAM_KEY).append(".").append(keyName).append(")");
                target.put(keyName, this.likeValueHandler(fieldValue));
                break;
            default:
                throw new SqlException("不支持查询条件");
        }

        return conditionExp.toString();
    }


    /**
     * 获取索引key
     *
     * @param doc
     * @return
     */
    protected IndexData getIndexData(Map<String, Object> doc, Index index) {
        Map<String, Object> indexData = Maps.newHashMap();

        for (String column : index.getColumns()) {
            Object o = doc.get(column);
            Assert.notNull(o, index.getName() + "值为空，字段名:" + column);
            indexData.put(column, o);
        }

        return new IndexData(this.getKey(indexData), indexData);
    }

    /**
     * 获取转换器
     *
     * @param columnType
     * @return
     */
    protected Convert<?> getConvert(ColumnTypeEnum columnType) {
        Convert<?> convert = converts.get(columnType);
        return convert != null ? convert : DefaultConvert.getInstance();
    }

    /**
     * 获取键值
     *
     * @param data
     * @return
     */
    protected String getKey(Map<String, Object> data) {
        // 将 Map 按 ASCII 码排序
        Map<String, Object> sortedMap = new TreeMap<>(data);

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue().toString()).append("&");
        }

        return SecureUtil.md5(sb.toString());
    }

    /**
     * 获取组合条件
     *
     * @param inputMap
     * @param keysToCombine
     * @return
     */
    protected List<Map<String, Object>> generateCombinations(Map<String, Set<Object>> inputMap, Set<String> keysToCombine) {

        List<Map<String, Object>> result = Lists.newArrayList();
        if (MapUtil.isEmpty(inputMap) || CollUtil.isEmpty(keysToCombine)) {
            return result;
        }
        // Filter the keys based on the provided list
        List<String> keys = new ArrayList<>();
        for (String key : inputMap.keySet()) {
            if (CollUtil.isEmpty(keysToCombine) || keysToCombine.contains(key)) {
                keys.add(key);
            }
        }

        generateConditionCombinations(inputMap, keys, 0, new HashMap<>(), result);
        return result;
    }

    /**
     * 生成条件组合键值
     *
     * @param inputMap
     * @param keys
     * @param index
     * @param currentCombination
     * @param result
     */
    private void generateConditionCombinations(Map<String, Set<Object>> inputMap, List<String> keys, int index, Map<String, Object> currentCombination, List<Map<String, Object>> result) {
        if (index == keys.size()) {
            result.add(new HashMap<>(currentCombination));
            return;
        }

        String currentKey = keys.get(index);
        Set<Object> values = inputMap.get(currentKey);

        for (Object value : values) {
            currentCombination.put(currentKey, value);
            generateConditionCombinations(inputMap, keys, index + 1, currentCombination, result);
            currentCombination.remove(currentKey); // Backtrack
        }
    }


    /**
     * like字段处理，处理掉百分号
     *
     * @param value
     * @return
     */
    private String likeValueHandler(Object value) {
        Assert.notNull(value, "模糊查询值为空");

        String like = value.toString().trim();
        if (StrUtil.startWith(like, "%")) {
            like = StrUtil.sub(like, 1, like.length());
        }

        if (StrUtil.endWith(like, "%")) {
            like = StrUtil.sub(like, 0, like.length() - 1);
        }

        Assert.hasText(like, "模糊查询值为空");
        return like;
    }

    /**
     * 获取聚合计算结果
     *
     * @param result
     * @param aggregateEnum
     * @param name
     * @return
     */
    private Object aggregateCalculate(Collection<Map<String, Object>> result, AggregateEnum aggregateEnum, String name) {
        switch (aggregateEnum) {
            case COUNT:
                return result.size();
            case MAX:
                return result.stream().filter(map -> map.get(name) != null).max(new MapComparator(name)).get().get(name);
            case MIN:
                return result.stream().filter(map -> map.get(name) != null).min(new MapComparator(name)).get().get(name);
            case AVG:
                return result.stream().filter(map -> cn.hutool.core.convert.Convert.toDouble(map.get(name)) != null).mapToDouble(map -> cn.hutool.core.convert.Convert.toDouble(map.get(name))).average().orElse(0D);
            case SUM:
                return result.stream().filter(map -> map.get(name) != null).mapToDouble(map -> NumberUtil.parseDouble(map.get(name) != null ? map.get(name).toString() : StrUtil.EMPTY)).sum();
        }

        return null;
    }
}
