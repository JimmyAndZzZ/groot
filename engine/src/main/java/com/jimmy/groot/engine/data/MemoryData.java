package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.googlecode.aviator.AviatorEvaluator;
import com.jimmy.groot.engine.data.other.IndexData;
import com.jimmy.groot.engine.data.memory.MemoryFragment;
import com.jimmy.groot.engine.data.memory.MemoryPartition;
import com.jimmy.groot.engine.data.other.ConditionExpression;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.sql.core.Condition;
import com.jimmy.groot.sql.element.ConditionElement;
import com.jimmy.groot.sql.element.QueryElement;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        IndexData uniqueData = super.getIndexData(doc, super.uniqueIndex);
        IndexData partitionData = super.getIndexData(doc, super.partitionIndex);

        String uniqueDataKey = uniqueData.getKey();
        String partitionDataKey = partitionData.getKey();

        partitions.computeIfAbsent(partitionDataKey, s -> new MemoryPartition(partitionDataKey, partitionData.getData()));
        partitions.get(partitionDataKey).save(uniqueDataKey, MemoryFragment.build(uniqueDataKey, serializer, uniqueData.getData()).writeMemory(doc));
    }

    @Override
    public void remove(Map<String, Object> doc) {
        IndexData uniqueData = super.getIndexData(doc, super.uniqueIndex);
        IndexData partitionData = super.getIndexData(doc, super.partitionIndex);

        MemoryPartition memoryPartition = partitions.get(partitionData.getKey());
        if (memoryPartition != null) {
            memoryPartition.remove(uniqueData.getKey());
        }
    }

    @Override
    protected Collection<Map<String, Object>> queryList(QueryElement queryElement) throws Exception {
        int end = queryElement.getEnd();
        int start = queryElement.getStart();
        boolean isFindAll = queryElement.isSelectAll();
        boolean allColumn = queryElement.isAllColumn();
        Map<String, Map<String, Object>> data = Maps.newHashMap();
        boolean withoutCondition = queryElement.isWithoutCondition();
        Set<String> needColumnNames = queryElement.getNeedColumnNames();
        List<ConditionElement> conditionElements = queryElement.getConditionElements();
        //超出范围
        int sum = partitions.values().stream().mapToInt(MemoryPartition::count).sum();
        if (!isFindAll && start > sum) {
            return Lists.newArrayList();
        }
        //无条件查询
        if (withoutCondition || CollUtil.isEmpty(conditionElements)) {
            return this.queryWithoutCondition(isFindAll, start, end, needColumnNames, allColumn);
        }
        //已处理分区
        AtomicInteger total = new AtomicInteger(0);
        Set<String> processPartitionCodes = Sets.newHashSet();
        Map<String, Map<String, Object>> records = Maps.newHashMap();
        //获取条件集合
        ConditionExpression conditionExpression = this.getConditionCollect(conditionElements);

        for (ConditionElement conditionElement : conditionElements) {
            Set<String> uniqueCodes = conditionElement.getUniqueCodes();
            Set<String> partitionCodes = conditionElement.getPartitionCodes();
            //全分区
            if (CollUtil.isEmpty(partitionCodes)) {
                for (MemoryPartition memoryPartition : partitions.values()) {
                    if (!processPartitionCodes.add(memoryPartition.getCode())) {
                        continue;
                    }

                    Collection<MemoryFragment> memoryFragments = memoryPartition.getFragments();
                    if (CollUtil.isEmpty(memoryFragments)) {
                        continue;
                    }
                    //无主键
                    if (CollUtil.isNotEmpty(uniqueCodes)) {
                        this.queryByUnique(conditionExpression,
                                uniqueCodes,
                                memoryPartition,
                                records,
                                isFindAll,
                                start,
                                end,
                                total,
                                allColumn,
                                needColumnNames);
                    } else {
                        this.queryAll(conditionExpression,
                                records,
                                isFindAll,
                                start,
                                end,
                                total,
                                allColumn,
                                needColumnNames,
                                memoryPartition);
                    }
                }
            } else {
                for (String partitionCode : partitionCodes) {
                    if (!processPartitionCodes.add(partitionCode)) {
                        continue;
                    }

                    MemoryPartition memoryPartition = partitions.get(partitionCode);
                    if (memoryPartition == null) {
                        continue;
                    }

                    Collection<MemoryFragment> memoryFragments = memoryPartition.getFragments();
                    if (CollUtil.isEmpty(memoryFragments)) {
                        continue;
                    }
                    //无主键
                    if (CollUtil.isNotEmpty(uniqueCodes)) {
                        this.queryByUnique(conditionExpression,
                                uniqueCodes,
                                memoryPartition,
                                records,
                                isFindAll,
                                start,
                                end,
                                total,
                                allColumn,
                                needColumnNames);
                    } else {
                        this.queryAll(conditionExpression,
                                records,
                                isFindAll,
                                start,
                                end,
                                total,
                                allColumn,
                                needColumnNames,
                                memoryPartition);
                    }
                }
            }

            if (!isFindAll && total.get() >= end) {
                break;
            }
        }

        return isFindAll ? records.values() : CollUtil.sub(records.values(), 0, end - start);
    }

    @Override
    protected Map<String, Object> uniqueKeyToData(String partitionKey, String uniqueKey, Set<String> needColumnNames, boolean isAllColumn) throws Exception {
        MemoryPartition memoryPartition = partitions.get(partitionKey);
        if (memoryPartition == null) {
            return null;
        }

        MemoryFragment fragmentByUniqueCode = memoryPartition.getFragmentByUniqueCode(uniqueKey);
        if (fragmentByUniqueCode == null) {
            return null;
        }

        Map<String, Object> data = fragmentByUniqueCode.getData();
        return isAllColumn ? data : super.columnFilter(data, needColumnNames);
    }

    @Override
    protected Map<String, Object> uniqueKeyToUniqueData(String partitionKey, String uniqueKey) throws Exception {
        MemoryPartition memoryPartition = partitions.get(partitionKey);
        if (memoryPartition == null) {
            return null;
        }

        MemoryFragment fragmentByUniqueCode = memoryPartition.getFragmentByUniqueCode(uniqueKey);
        if (fragmentByUniqueCode == null) {
            return null;
        }

        return fragmentByUniqueCode.getKey();
    }

    /**
     * 查询所有数据
     *
     * @param conditionExpression
     * @param records
     * @param isFindAll
     * @param start
     * @param end
     * @param total
     * @param allColumn
     * @param needColumnNames
     * @param memoryPartition
     */
    private void queryAll(ConditionExpression conditionExpression,
                          Map<String, Map<String, Object>> records,
                          boolean isFindAll,
                          int start,
                          int end,
                          AtomicInteger total,
                          boolean allColumn,
                          Set<String> needColumnNames,
                          MemoryPartition memoryPartition) throws Exception {
        //数据反序列化
        Collection<Map<String, Object>> allData = memoryPartition.getFragments().stream().map(MemoryFragment::getData).collect(Collectors.toList());

        for (Map<String, Object> data : allData) {
            if (!isFindAll && total.get() > end) {
                break;
            }

            IndexData indexData = super.getIndexData(data, super.uniqueIndex);

            String uniqueKey = indexData.getKey();

            if (records.containsKey(uniqueKey)) {
                continue;
            }

            super.filterAndPut(conditionExpression,
                    uniqueKey,
                    memoryPartition.getCode(),
                    total,
                    records,
                    isFindAll ? 0 : start,
                    needColumnNames,
                    allColumn);
        }
    }

    /**
     * 根据主键查询
     *
     * @param conditionExpression
     * @param uniqueCodes
     * @param memoryPartition
     * @param records
     * @param isFindAll
     * @param start
     * @param end
     * @param total
     * @param allColumn
     * @param needColumnNames
     */
    private void queryByUnique(ConditionExpression conditionExpression,
                               Collection<String> uniqueCodes,
                               MemoryPartition memoryPartition,
                               Map<String, Map<String, Object>> records,
                               boolean isFindAll,
                               int start,
                               int end,
                               AtomicInteger total,
                               boolean allColumn,
                               Set<String> needColumnNames) throws Exception {
        if (!isFindAll) {
            uniqueCodes.removeIf(uniqueCode -> memoryPartition.getFragmentByUniqueCode(uniqueCode) == null || records.containsKey(uniqueCode));

            if (CollUtil.isEmpty(uniqueCodes)) {
                return;
            }

            int andAdd = total.getAndAdd(uniqueCodes.size());
            uniqueCodes = CollUtil.sub(uniqueCodes, andAdd, end);
        }

        for (String uniqueCode : uniqueCodes) {
            Map<String, Object> data = memoryPartition.getFragmentByUniqueCode(uniqueCode).getData();
            if (data != null) {
                super.filterAndPut(conditionExpression,
                        uniqueCode,
                        memoryPartition.getCode(),
                        total,
                        records,
                        isFindAll ? 0 : start,
                        needColumnNames,
                        allColumn);
            }
        }
    }


    /**
     * 获取条件所有表达式
     *
     * @param conditionElements
     * @return
     */
    private ConditionExpression getConditionCollect(List<ConditionElement> conditionElements) {
        int i = 0;
        StringBuilder parentOtherExpression = new StringBuilder();
        StringBuilder parentUniqueExpression = new StringBuilder();
        ConditionExpression conditionExpression = new ConditionExpression();

        for (ConditionElement conditionElement : conditionElements) {
            List<Condition> conditions = conditionElement.getConditions();
            if (CollUtil.isNotEmpty(conditions)) {
                StringBuilder childOtherExpression = new StringBuilder();
                StringBuilder childUniqueExpression = new StringBuilder();

                for (Condition condition : conditions) {
                    Column column = super.columnMap.get(condition.getFieldName());

                    Boolean isUniqueKey = column.getIsUniqueKey();

                    String expCondition = super.getExpCondition(
                            column,
                            condition.getFieldValue(),
                            condition.getConditionEnum(),
                            isUniqueKey ? conditionExpression.getUniqueConditionArgument() : conditionExpression.getOtherConditionArgument(),
                            i++);

                    StringBuilder expression = isUniqueKey ? childUniqueExpression : childOtherExpression;

                    if (StrUtil.isNotBlank(expression)) {
                        expression.append(ConditionTypeEnum.AND.getExpression());
                    }

                    expression.append(expCondition);
                }

                if (StrUtil.isNotBlank(childOtherExpression)) {
                    if (StrUtil.isNotBlank(parentOtherExpression)) {
                        parentOtherExpression.append(ConditionTypeEnum.OR.getExpression());
                    }

                    parentOtherExpression.append("(").append(childOtherExpression).append(")");
                }

                if (StrUtil.isNotBlank(childUniqueExpression)) {
                    if (StrUtil.isNotBlank(parentUniqueExpression)) {
                        parentUniqueExpression.append(ConditionTypeEnum.OR.getExpression());
                    }

                    parentUniqueExpression.append("(").append(childUniqueExpression).append(")");
                }
            }
        }

        if (StrUtil.isNotBlank(parentOtherExpression)) {
            conditionExpression.setOtherExpression(AviatorEvaluator.compile(parentOtherExpression.toString()));
        }

        if (StrUtil.isNotBlank(parentUniqueExpression)) {
            conditionExpression.setUniqueExpression(AviatorEvaluator.compile(parentUniqueExpression.toString()));
        }

        return conditionExpression;
    }

    /**
     * 无条件获取数据
     *
     * @param isFindAll
     * @param start
     * @param end
     * @param needColumnNames
     * @param isAllColumn
     * @return
     */
    private Collection<Map<String, Object>> queryWithoutCondition(boolean isFindAll,
                                                                  int start,
                                                                  int end,
                                                                  Set<String> needColumnNames,
                                                                  boolean isAllColumn) {
        int i = 0;
        List<Map<String, Object>> result = Lists.newArrayList();

        for (MemoryPartition value : partitions.values()) {
            Collection<MemoryFragment> fragments = value.getFragments();

            if (CollUtil.isNotEmpty(fragments)) {
                for (MemoryFragment fragment : fragments) {
                    if (!isFindAll && i++ >= end) {
                        break;
                    }

                    Map<String, Object> data = fragment.getData();

                    if (isFindAll || i > start) {
                        result.add(isAllColumn ? data : super.columnFilter(data, needColumnNames));
                    }
                }
            }
        }

        return result;
    }

}
