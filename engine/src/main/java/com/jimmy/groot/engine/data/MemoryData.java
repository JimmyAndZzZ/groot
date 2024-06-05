package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.data.memory.MemoryFragment;
import com.jimmy.groot.engine.data.memory.MemoryPartition;
import com.jimmy.groot.engine.data.other.ConditionPart;
import com.jimmy.groot.engine.exception.SqlException;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.platform.other.Assert;
import com.jimmy.groot.sql.core.AggregateEnum;
import com.jimmy.groot.sql.core.AggregateFunction;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import com.jimmy.groot.sql.other.MapComparator;
import lombok.Data;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.jimmy.groot.platform.constant.ClientConstant.SOURCE_PARAM_KEY;
import static com.jimmy.groot.platform.constant.ClientConstant.TARGET_PARAM_KEY;

public class MemoryData extends AbstractData {

    private Serializer serializer;

    private ConcurrentMap<String, MemoryPartition> partitions;

    private MemoryData(List<Column> columns) {
        super(columns);
    }

    public static MemoryData build(Serializer serializer, List<Column> columns) {
        MemoryData table = new MemoryData(columns);
        table.serializer = serializer;
        table.partitions = Maps.newConcurrentMap();
        return table;
    }

    @Override
    public void save(Map<String, Object> doc) {
        IndexData uniqueData = super.getUniqueData(doc);
        IndexData partitionData = super.getPartitionData(doc);

        String uniqueDataKey = uniqueData.getKey();
        String partitionDataKey = partitionData.getKey();

        partitions.computeIfAbsent(partitionDataKey, s -> new MemoryPartition(partitionDataKey, partitionData.getData()));
        partitions.get(partitionDataKey).save(uniqueDataKey, MemoryFragment.build(uniqueDataKey, serializer, uniqueData.getData()).writeMemory(doc));
    }

    @Override
    public void remove(Map<String, Object> doc) {
        IndexData uniqueData = super.getUniqueData(doc);
        IndexData partitionData = super.getPartitionData(doc);

        MemoryPartition memoryPartition = partitions.get(partitionData.getKey());
        if (memoryPartition != null) {
            memoryPartition.remove(uniqueData.getKey());
        }
    }

    @Override
    public Collection<Map<String, Object>> page(QueryPlus queryPlus, Integer pageNo, Integer pageSize) {
        return this.aggregateHandler(queryPlus, this.queryByCondition(queryPlus, pageNo * pageSize, pageNo * pageSize + pageSize));
    }

    @Override
    public Collection<Map<String, Object>> list(QueryPlus queryPlus) {
        return this.aggregateHandler(queryPlus, this.queryByCondition(queryPlus, -1, -1));
    }

    /**
     * 聚合函数处理
     *
     * @param result
     * @return
     */
    private Collection<Map<String, Object>> aggregateHandler(QueryPlus queryPlus, Collection<Map<String, Object>> result) {
        Set<String> select = queryPlus.getSelect();
        List<String> groupBy = queryPlus.getGroupBy();
        List<AggregateFunction> aggregateFunctions = queryPlus.getAggregateFunctions();

        if (CollUtil.isEmpty(result)) {
            return result;
        }

        if (CollUtil.isEmpty(groupBy) && CollUtil.isEmpty(aggregateFunctions)) {
            return result;
        }

        if (CollUtil.isEmpty(groupBy) && CollUtil.isNotEmpty(aggregateFunctions)) {
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
        //groupby
        Map<String, List<Map<String, Object>>> groupby = result.stream().collect(Collectors.groupingBy(m -> this.getGroupKey(m, groupBy)));

        List<Map<String, Object>> list = Lists.newArrayList();

        for (Map.Entry<String, List<Map<String, Object>>> entry : groupby.entrySet()) {
            List<Map<String, Object>> mapValue = entry.getValue();

            Map<String, Object> map = mapValue.stream().findFirst().get();
            Map<String, Object> data = Maps.newHashMap();

            for (String s : groupBy) {
                data.put(s, map.get(s));
            }

            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                data.put(aggregateFunction.getAlias(), this.aggregateCalculate(mapValue, aggregateFunction.getAggregateType(), aggregateFunction.getColumn()));
            }

            list.add(data);
        }

        return list;
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

    /**
     * 获取groupby key
     *
     * @return
     */
    private String getGroupKey(Map<String, Object> map, List<String> groupBy) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < groupBy.size(); i++) {
            if (i > 0) {
                sb.append(":");
            }

            String s = groupBy.get(i);
            Object o = map.get(s);
            sb.append(o != null ? o.toString() : "NULL");
        }

        return sb.toString();
    }

    /**
     * @param queryPlus
     * @return
     */
    private Collection<Map<String, Object>> queryByCondition(QueryPlus queryPlus, int start, int end) {
        int sum = partitions.values().stream().mapToInt(MemoryPartition::count).sum();
        if (start >= sum) {
            return Lists.newArrayList();
        }

        boolean isFindAll = start < 0 && end < 0;
        Set<String> processUniqueCodes = Sets.newHashSet();
        Set<String> processPartitionCodes = Sets.newHashSet();
        Map<String, Map<String, Object>> data = Maps.newHashMap();
        //解析查询条件
        List<ConditionPart> conditionParts = this.analyzeCondition(queryPlus.getConditionGroups());
        //寻找分区
        List<Route> routes = this.findRoute(conditionParts);
        //获取条件汇总
        ConditionPart conditionCollect = this.getConditionCollect(conditionParts);
        //拼接所有查询条件
        Expression expression = AviatorEvaluator.compile(conditionCollect.getFullExpression());

        for (Route route : routes) {
            Boolean isAll = route.getIsAll();
            ConditionPart conditionPart = route.getConditionPart();
            List<MemoryPartition> routeMemoryPartitions = route.getMemoryPartitions();

            if (isAll) {
                for (MemoryPartition routeMemoryPartition : routeMemoryPartitions) {
                    if (!processPartitionCodes.add(routeMemoryPartition.getCode())) {
                        continue;
                    }

                    Collection<MemoryFragment> memoryFragments = routeMemoryPartition.getFragments();
                    if (CollUtil.isEmpty(memoryFragments)) {
                        continue;
                    }

                    for (MemoryFragment memoryFragment : memoryFragments) {
                        this.filter(memoryFragment.getCode(), memoryFragment.getData(), conditionCollect.getConditionArgument(), expression, isFindAll, data, processUniqueCodes, start);

                        if (!isFindAll && processUniqueCodes.size() == end) {
                            return data.values();
                        }
                    }
                }
            } else {
                //符合的唯一键值
                Set<String> uniqueCodes = conditionPart.getUniqueCodes();
                //包含主键值条件表达式
                String fullExpression = conditionPart.getFullExpression();
                String uniqueExpression = conditionPart.getUniqueExpression();
                Map<String, Object> conditionArgument = conditionPart.getConditionArgument();
                Expression tempFullExpression = AviatorEvaluator.compile(fullExpression);

                Set<String> tempProcessUniqueCodes = Sets.newHashSet();

                for (MemoryPartition routeMemoryPartition : routeMemoryPartitions) {
                    if (processPartitionCodes.contains(routeMemoryPartition.getCode())) {
                        continue;
                    }
                    //主键值范围
                    if (CollUtil.isNotEmpty(uniqueCodes)) {
                        for (String uniqueCode : uniqueCodes) {
                            MemoryFragment memoryFragmentByUniqueCode = routeMemoryPartition.getFragmentByUniqueCode(uniqueCode);
                            if (memoryFragmentByUniqueCode != null && tempProcessUniqueCodes.add(uniqueCode)) {
                                this.filter(uniqueCode, memoryFragmentByUniqueCode.getData(), conditionArgument, tempFullExpression, isFindAll, data, processUniqueCodes, start);

                                if (!isFindAll && processUniqueCodes.size() == end) {
                                    return data.values();
                                }
                            }
                        }
                    }

                    if (StrUtil.isNotBlank(uniqueExpression)) {
                        Expression tempUniqueExpression = AviatorEvaluator.compile(uniqueExpression);

                        Collection<MemoryFragment> memoryFragments = routeMemoryPartition.getFragments();
                        if (CollUtil.isEmpty(memoryFragments)) {
                            continue;
                        }

                        for (MemoryFragment memoryFragment : memoryFragments) {
                            Map<String, Object> param = Maps.newHashMap();
                            param.put(SOURCE_PARAM_KEY, memoryFragment.getKey());
                            param.put(TARGET_PARAM_KEY, conditionArgument);
                            Boolean flag = cn.hutool.core.convert.Convert.toBool(tempUniqueExpression.execute(param), false);

                            if (flag) {
                                this.filter(memoryFragment.getCode(), memoryFragment.getData(), conditionArgument, tempFullExpression, isFindAll, data, processUniqueCodes, start);

                                if (!isFindAll && processUniqueCodes.size() == end) {
                                    return data.values();
                                }
                            }
                        }
                    }
                }
            }
        }

        return data.values();
    }

    /**
     * 数据过滤
     *
     * @param uniqueCode
     * @param d
     * @param conditionArgument
     * @param expression
     * @param isFindAll
     * @param data
     * @param processUniqueCodes
     * @param start
     */
    private void filter(String uniqueCode, Map<String, Object> d, Map<String, Object> conditionArgument, Expression expression, Boolean isFindAll, Map<String, Map<String, Object>> data, Set<String> processUniqueCodes, int start) {
        Map<String, Object> param = Maps.newHashMap();
        param.put(SOURCE_PARAM_KEY, d);
        param.put(TARGET_PARAM_KEY, conditionArgument);
        Boolean flag = cn.hutool.core.convert.Convert.toBool(expression.execute(param), false);
        if (flag && isFindAll) {
            data.put(uniqueCode, d);
        } else {
            if (!processUniqueCodes.add(uniqueCode)) {
                return;
            }

            if (processUniqueCodes.size() > start) {
                data.put(uniqueCode, d);
            }
        }
    }

    /**
     * 获取条件所有表达式
     *
     * @param conditionParts
     * @return
     */
    private ConditionPart getConditionCollect(List<ConditionPart> conditionParts) {
        ConditionPart collect = new ConditionPart();
        StringBuilder conditionExp = new StringBuilder();

        for (ConditionPart conditionPart : conditionParts) {
            String fullExpression = conditionPart.getFullExpression();
            if (StrUtil.isEmpty(fullExpression)) {
                continue;
            }

            if (StrUtil.isNotBlank(conditionExp)) {
                conditionExp.append(ConditionTypeEnum.OR.getExpression());
            }

            conditionExp.append("(").append(fullExpression).append(")");
            collect.getConditionArgument().putAll(conditionPart.getConditionArgument());
        }

        collect.setFullExpression(conditionExp.toString());
        return collect;
    }

    /**
     * 寻找分区
     *
     * @param conditionParts
     * @return
     */
    private List<Route> findRoute(List<ConditionPart> conditionParts) {
        List<Route> routes = Lists.newArrayList();

        for (ConditionPart conditionPart : conditionParts) {
            Set<String> partitionCodes = conditionPart.getPartitionCodes();
            String partitionExpression = conditionPart.getPartitionExpression();
            Map<String, Object> conditionArgument = conditionPart.getConditionArgument();

            Route route = new Route();
            route.setConditionPart(conditionPart);

            if (CollUtil.isEmpty(partitionCodes) && StrUtil.isEmpty(partitionExpression)) {
                route.setIsAll(true);
                route.getMemoryPartitions().addAll(this.partitions.values());
                routes.add(route);
                continue;
            }

            if (CollUtil.isNotEmpty(partitionCodes)) {
                for (String partitionCode : partitionCodes) {
                    MemoryPartition memoryPartition = this.partitions.get(partitionCode);
                    if (memoryPartition == null) {
                        route.setIsAll(true);
                        route.getMemoryPartitions().addAll(this.partitions.values());
                        routes.add(route);
                        break;
                    }

                    route.getMemoryPartitions().add(memoryPartition);
                }
            }

            if (StrUtil.isNotBlank(partitionExpression)) {
                for (MemoryPartition value : this.partitions.values()) {
                    Expression expression = AviatorEvaluator.compile(partitionExpression);

                    Map<String, Object> param = Maps.newHashMap();
                    param.put(SOURCE_PARAM_KEY, value.getKey());
                    param.put(TARGET_PARAM_KEY, conditionArgument);
                    Boolean flag = cn.hutool.core.convert.Convert.toBool(expression.execute(param), false);
                    if (flag) {
                        route.getMemoryPartitions().add(value);
                    }
                }
            }
        }

        return routes;
    }


    /**
     * 获取条件分片
     *
     * @param conditionGroups
     * @return
     */
    private List<ConditionPart> analyzeCondition(List<List<Condition>> conditionGroups) {
        int i = 0;
        List<ConditionPart> parts = Lists.newArrayList();
        Set<String> uniqueIndexColumns = uniqueIndex.getColumns();
        Set<String> partitionIndexColumns = partitionIndex.getColumns();
        Map<String, Column> columnMap = columns.stream().collect(Collectors.toMap(Column::getName, g -> g));
        //遍历关联关系
        for (List<Condition> groupConditions : conditionGroups) {
            if (CollUtil.isNotEmpty(groupConditions)) {
                ConditionPart part = new ConditionPart();
                StringBuilder fullExpression = new StringBuilder();
                StringBuilder uniqueExpression = new StringBuilder();
                StringBuilder partitionExpression = new StringBuilder();
                Map<String, Set<Object>> keyConditionValue = Maps.newHashMap();

                for (Condition condition : groupConditions) {
                    String fieldName = condition.getFieldName();

                    Column column = columnMap.get(fieldName);
                    Assert.notNull(column, fieldName + "参数不存在");

                    String expCondition = this.getExpCondition(column, keyConditionValue, condition.getFieldValue(), condition.getConditionEnum(), part.getConditionArgument(), i++);

                    if (StrUtil.isNotBlank(fullExpression)) {
                        fullExpression.append(ConditionTypeEnum.AND.getExpression());
                    }

                    fullExpression.append(expCondition);

                    if (uniqueIndex.contain(fieldName)) {
                        if (StrUtil.isNotBlank(uniqueExpression)) {
                            uniqueExpression.append(ConditionTypeEnum.AND.getExpression());
                        }

                        uniqueExpression.append(expCondition);
                    }

                    if (partitionIndex.contain(fieldName)) {
                        if (StrUtil.isNotBlank(partitionExpression)) {
                            partitionExpression.append(ConditionTypeEnum.AND.getExpression());
                        }

                        partitionExpression.append(expCondition);
                    }
                }

                part.setFullExpression(fullExpression.toString());
                part.setUniqueExpression(uniqueExpression.toString());
                part.setPartitionExpression(partitionExpression.toString());
                //唯一键值计算
                if (this.mapContainKeys(keyConditionValue, uniqueIndexColumns)) {
                    List<Map<String, Object>> result = super.generateCombinations(keyConditionValue, uniqueIndexColumns);

                    if (CollUtil.isNotEmpty(result)) {
                        for (Map<String, Object> map : result) {
                            part.getUniqueCodes().add(super.getKey(map));
                        }
                    }
                }
                //分区键值计算
                if (this.mapContainKeys(keyConditionValue, partitionIndexColumns)) {
                    List<Map<String, Object>> result = super.generateCombinations(keyConditionValue, partitionIndexColumns);

                    if (CollUtil.isNotEmpty(result)) {
                        for (Map<String, Object> map : result) {
                            part.getPartitionCodes().add(super.getKey(map));
                        }
                    }
                }

                parts.add(part);
            }
        }

        return parts;
    }

    /**
     * 判断map是否包含keys里所有元素
     *
     * @param map
     * @param keys
     * @return
     */
    private boolean mapContainKeys(Map<String, ?> map, Collection<String> keys) {
        for (String key : keys) {
            if (!map.containsKey(key)) {
                return false;
            }
        }

        return true;
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
    private String getExpCondition(Column column, Map<String, Set<Object>> keyConditionValue, Object fieldValue, ConditionEnum conditionEnum, Map<String, Object> target, int i) {
        String name = column.getName();
        String keyName = name + "$" + i;
        StringBuilder conditionExp = new StringBuilder();
        Convert<?> convert = super.getConvert(column.getColumnType());
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

    @Data
    private static class Route implements Serializable {

        private Boolean isAll = false;

        private ConditionPart conditionPart;

        private List<MemoryPartition> memoryPartitions = Lists.newArrayList();
    }
}
