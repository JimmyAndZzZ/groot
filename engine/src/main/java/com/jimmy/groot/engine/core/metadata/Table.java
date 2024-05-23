package com.jimmy.groot.engine.core.metadata;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.core.other.ConditionPart;
import com.jimmy.groot.engine.core.other.Fragment;
import com.jimmy.groot.engine.enums.ColumnTypeEnum;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.engine.other.Assert;
import com.jimmy.groot.engine.store.SegmentSerializer;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.core.ConditionGroup;
import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.jimmy.groot.engine.other.Constant.SOURCE_PARAM_KEY;
import static com.jimmy.groot.engine.other.Constant.TARGET_PARAM_KEY;

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

        partitions.computeIfAbsent(partitionKey, s -> new Partition(partitionData));
        partitions.get(partitionKey).save(uniqueKey, Fragment.build(segmentSerializer, uniqueData).writeDisk(diskData).writeMemory(memoryData));
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

                    String expCondition = this.getExpCondition(column,
                            keyConditionValue,
                            condition.getFieldValue(),
                            condition.getConditionEnum(),
                            part.getConditionArgument(),
                            i++);

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
                parts.add(part);
            }
        }

        return parts;
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
    private String getExpCondition(Column column,
                                   Map<String, Set<Object>> keyConditionValue,
                                   Object fieldValue,
                                   ConditionEnum conditionEnum,
                                   Map<String, Object> target,
                                   int i) {
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
}
