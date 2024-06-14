package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.data.memory.MemoryFragment;
import com.jimmy.groot.engine.data.memory.MemoryPartition;
import com.jimmy.groot.engine.data.other.IndexData;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.platform.base.Serializer;
import com.jimmy.groot.sql.element.ConditionElement;
import com.jimmy.groot.sql.element.QueryElement;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

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


        return data.values();
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
