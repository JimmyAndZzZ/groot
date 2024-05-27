package com.jimmy.groot.engine.core.metadata;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.core.other.ConditionPart;
import com.jimmy.groot.engine.core.other.Fragment;
import com.jimmy.groot.engine.enums.ColumnTypeEnum;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.engine.segment.SegmentSerializer;
import com.jimmy.groot.platform.other.Assert;
import com.jimmy.groot.sql.core.*;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import com.jimmy.groot.sql.other.MapComparator;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.jimmy.groot.platform.constant.ClientConstant.SOURCE_PARAM_KEY;
import static com.jimmy.groot.platform.constant.ClientConstant.TARGET_PARAM_KEY;

public class Table implements Serializable {

    @Getter
    private String schema;

    @Getter
    private String tableName;

    private SegmentSerializer segmentSerializer;

    private List<Column> columns = Lists.newArrayList();

    private Set<String> uniqueColumns = Sets.newHashSet();

    private Set<String> partitionColumns = Sets.newHashSet();

    private ConcurrentMap<String, Partition> partitions = Maps.newConcurrentMap();

    private Table() {

    }

    public static Table build(SegmentSerializer segmentSerializer, String schema, String tableName, List<Column> columns) {
        Assert.hasText(schema, "schema为空");
        Assert.hasText(tableName, "表名为空");
        Assert.notEmpty(columns, "表字段为空");

        Table table = new Table();
        table.schema = schema;
        table.tableName = tableName;
        table.segmentSerializer = segmentSerializer;

        for (Column column : columns) {
            String name = column.getName();

            if (column.getIsPartitionKey()) {
                table.partitionColumns.add(name);
            }

            if (column.getIsUniqueKey()) {
                table.uniqueColumns.add(name);
            }
        }

        Assert.notEmpty(table.partitionColumns, "分区键为空");
        Assert.notEmpty(table.uniqueColumns, "唯一键为空");
        return table;
    }

    public void save(Map<String, Object> doc) {
        Map<String, Object> diskData = Maps.newHashMap();
        Map<String, Object> memoryData = Maps.newHashMap();
        Map<String, Object> uniqueData = Maps.newHashMap();
        Map<String, Object> partitionData = Maps.newHashMap();

        for (Column column : columns) {
            String name = column.getName();
            ColumnTypeEnum columnType = column.getColumnType();

            Object o = doc.get(name);

            if (column.getIsPartitionKey()) {
                Assert.notNull(o, "分区键值为空，字段名:" + name);
                partitionData.put(name, o);
            }

            if (column.getIsUniqueKey()) {
                Assert.notNull(o, "唯一键值为空，字段名:" + name);
                uniqueData.put(name, o);
            }

            if (columnType.getIsStoreMemory()) {
                memoryData.put(name, o);
            } else {
                diskData.put(name, o);
            }
        }

        String uniqueKey = this.getKey(uniqueData);
        String partitionKey = this.getKey(partitionData);

        partitions.computeIfAbsent(partitionKey, s -> new Partition(partitionKey, partitionData));
        partitions.get(partitionKey).save(uniqueKey, Fragment.build(uniqueKey, segmentSerializer, uniqueData).writeDisk(diskData).writeMemory(memoryData));
    }

    public void remove(Map<String, Object> doc) {
        Map<String, Object> uniqueData = Maps.newHashMap();
        Map<String, Object> partitionData = Maps.newHashMap();

        for (Column column : columns) {
            String name = column.getName();

            Object o = doc.get(name);

            if (column.getIsPartitionKey()) {
                Assert.notNull(o, "分区键值为空，字段名:" + name);
                partitionData.put(name, o);
            }

            if (column.getIsUniqueKey()) {
                Assert.notNull(o, "唯一键值为空，字段名:" + name);
                uniqueData.put(name, o);
            }
        }

        Partition partition = partitions.get(this.getKey(partitionData));
        if (partition != null) {
            partition.remove(this.getKey(uniqueData));
        }
    }

    public Collection<Map<String, Object>> page(QueryPlus queryPlus, Integer pageNo, Integer pageSize) {
        return this.aggregateHandler(queryPlus, this.queryByCondition(queryPlus, pageNo * pageSize, pageNo * pageSize + pageSize));
    }

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
                return result.stream()
                        .filter(map -> map.get(name) != null)
                        .max(new MapComparator(name)).get().get(name);
            case MIN:
                return result.stream()
                        .filter(map -> map.get(name) != null)
                        .min(new MapComparator(name)).get().get(name);
            case AVG:
                return result.stream()
                        .filter(map -> cn.hutool.core.convert.Convert.toDouble(map.get(name)) != null)
                        .mapToDouble(map -> cn.hutool.core.convert.Convert.toDouble(map.get(name))).average().orElse(0D);
            case SUM:
                return result.stream()
                        .filter(map -> map.get(name) != null)
                        .mapToDouble(map -> NumberUtil.parseDouble(map.get(name) != null ? map.get(name).toString() : StrUtil.EMPTY)).sum();
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
        int sum = partitions.values().stream().mapToInt(Partition::count).sum();
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
            List<Partition> routePartitions = route.getPartitions();

            if (isAll) {
                for (Partition routePartition : routePartitions) {
                    if (!processPartitionCodes.add(routePartition.getCode())) {
                        continue;
                    }

                    Collection<Fragment> fragments = routePartition.getFragments();
                    if (CollUtil.isEmpty(fragments)) {
                        continue;
                    }

                    for (Fragment fragment : fragments) {
                        this.filter(fragment.getCode(),
                                fragment.getData(),
                                conditionCollect.getConditionArgument(),
                                expression,
                                isFindAll,
                                data,
                                processUniqueCodes,
                                start);

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

                for (Partition routePartition : routePartitions) {
                    if (processPartitionCodes.contains(routePartition.getCode())) {
                        continue;
                    }
                    //主键值范围
                    if (CollUtil.isNotEmpty(uniqueCodes)) {
                        for (String uniqueCode : uniqueCodes) {
                            Fragment fragmentByUniqueCode = routePartition.getFragmentByUniqueCode(uniqueCode);
                            if (fragmentByUniqueCode != null && tempProcessUniqueCodes.add(uniqueCode)) {
                                this.filter(uniqueCode,
                                        fragmentByUniqueCode.getData(),
                                        conditionArgument,
                                        tempFullExpression,
                                        isFindAll,
                                        data,
                                        processUniqueCodes,
                                        start);

                                if (!isFindAll && processUniqueCodes.size() == end) {
                                    return data.values();
                                }
                            }
                        }
                    }

                    if (StrUtil.isNotBlank(uniqueExpression)) {
                        Expression tempUniqueExpression = AviatorEvaluator.compile(uniqueExpression);

                        Collection<Fragment> fragments = routePartition.getFragments();
                        if (CollUtil.isEmpty(fragments)) {
                            continue;
                        }

                        for (Fragment fragment : fragments) {
                            Map<String, Object> param = Maps.newHashMap();
                            param.put(SOURCE_PARAM_KEY, fragment.getKey());
                            param.put(TARGET_PARAM_KEY, conditionArgument);
                            Boolean flag = cn.hutool.core.convert.Convert.toBool(tempUniqueExpression.execute(param), false);

                            if (flag) {
                                this.filter(fragment.getCode(),
                                        fragment.getData(),
                                        conditionArgument,
                                        tempFullExpression,
                                        isFindAll,
                                        data,
                                        processUniqueCodes,
                                        start);

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
    private void filter(String uniqueCode,
                        Map<String, Object> d,
                        Map<String, Object> conditionArgument,
                        Expression expression,
                        Boolean isFindAll,
                        Map<String, Map<String, Object>> data,
                        Set<String> processUniqueCodes,
                        int start) {
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
                route.getPartitions().addAll(this.partitions.values());
                routes.add(route);
                continue;
            }

            if (CollUtil.isNotEmpty(partitionCodes)) {
                for (String partitionCode : partitionCodes) {
                    Partition partition = this.partitions.get(partitionCode);
                    if (partition == null) {
                        route.setIsAll(true);
                        route.getPartitions().addAll(this.partitions.values());
                        routes.add(route);
                        break;
                    }

                    route.getPartitions().add(partition);
                }
            }

            if (StrUtil.isNotBlank(partitionExpression)) {
                for (Partition value : this.partitions.values()) {
                    Expression expression = AviatorEvaluator.compile(partitionExpression);

                    Map<String, Object> param = Maps.newHashMap();
                    param.put(SOURCE_PARAM_KEY, value.getKey());
                    param.put(TARGET_PARAM_KEY, conditionArgument);
                    Boolean flag = cn.hutool.core.convert.Convert.toBool(expression.execute(param), false);
                    if (flag) {
                        route.getPartitions().add(value);
                    }
                }
            }
        }

        return routes;
    }


    /**
     * 获取键值
     *
     * @param data
     * @return
     */
    private String getKey(Map<String, Object> data) {
        // 将 Map 按 ASCII 码排序
        Map<String, Object> sortedMap = new TreeMap<>(data);

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue().toString()).append("&");
        }

        return SecureUtil.md5(sb.toString());
    }


    /**
     * 获取表达式
     */
    private List<ConditionPart> analyzeCondition(List<ConditionGroup> conditionGroups) {
        int i = 0;
        List<ConditionPart> parts = Lists.newArrayList();
        Map<String, Column> columnMap = columns.stream().collect(Collectors.toMap(Column::getName, g -> g));
        //遍历关联关系
        for (ConditionGroup conditionGroup : conditionGroups) {
            List<Condition> groupConditions = conditionGroup.getConditions();

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

                    if (uniqueColumns.contains(fieldName)) {
                        if (StrUtil.isNotBlank(uniqueExpression)) {
                            uniqueExpression.append(ConditionTypeEnum.AND.getExpression());
                        }

                        uniqueExpression.append(expCondition);
                    }

                    if (partitionColumns.contains(fieldName)) {
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
                if (this.mapContainKeys(keyConditionValue, uniqueColumns)) {
                    List<Map<String, Object>> result = this.generateCombinations(keyConditionValue, uniqueColumns);

                    if (CollUtil.isNotEmpty(result)) {
                        for (Map<String, Object> map : result) {
                            part.getUniqueCodes().add(this.getKey(map));
                        }
                    }
                }
                //分区键值计算
                if (this.mapContainKeys(keyConditionValue, partitionColumns)) {
                    List<Map<String, Object>> result = this.generateCombinations(keyConditionValue, partitionColumns);

                    if (CollUtil.isNotEmpty(result)) {
                        for (Map<String, Object> map : result) {
                            part.getPartitionCodes().add(this.getKey(map));
                        }
                    }
                }

                parts.add(part);
            }
        }

        return parts;
    }


    private List<Map<String, Object>> generateCombinations
            (Map<String, Set<Object>> inputMap, Set<String> keysToCombine) {

        List<Map<String, Object>> result = Lists.newArrayList();
        if (MapUtil.isEmpty(inputMap) || CollUtil.isEmpty(keysToCombine)) {
            return result;
        }
        // Filter the keys based on the provided list
        List<String> keys = new ArrayList<>();
        for (String key : inputMap.keySet()) {
            if (keysToCombine.contains(key)) {
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
    private void generateConditionCombinations(Map<String, Set<Object>> inputMap, List<String> keys,
                                               int index, Map<String, Object> currentCombination, List<Map<String, Object>> result) {
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
    private String getExpCondition(Column column, Map<String, Set<Object>> keyConditionValue, Object
            fieldValue, ConditionEnum conditionEnum, Map<String, Object> target, int i) {
        String name = column.getName();
        String keyName = name + "$" + i;
        StringBuilder conditionExp = new StringBuilder();
        Convert<?> convert = column.getColumnType().getConvert();
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
                    throw new IllegalArgumentException("not in 集合为空");
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
                throw new EngineException("不支持查询条件");
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

        private List<Partition> partitions = Lists.newArrayList();
    }
}
