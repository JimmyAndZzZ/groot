package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
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
import com.jimmy.groot.sql.element.ConditionElement;
import com.jimmy.groot.sql.element.QueryElement;
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
    public Collection<Map<String, Object>> query(QueryElement queryElement) {
        return null;
    }

    /**
     * @param queryElement
     * @return
     */
    private Collection<Map<String, Object>> queryByCondition(QueryElement queryElement) {
        int end = queryElement.getEnd();
        int start = queryElement.getStart();
        boolean isFindAll = queryElement.isSelectAll();
        boolean allColumn = queryElement.isAllColumn();
        Map<String, Map<String, Object>> data = Maps.newHashMap();
        boolean withoutCondition = queryElement.isWithoutCondition();
        Set<String> needColumnNames = queryElement.getNeedColumnNames();
        ConditionElement conditionElement = queryElement.getConditionElement();
        //超出范围
        int sum = partitions.values().stream().mapToInt(MemoryPartition::count).sum();
        if (!isFindAll && start >= sum) {
            return Lists.newArrayList();
        }
        //全分区查询
        if (withoutCondition) {
            for (MemoryPartition routeMemoryPartition : partitions.values()) {
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
        }

        return data.values();
    }

    /**
     * 分区查询
     *
     * @param routeMemoryPartition
     * @return
     */
    private List<Map<String, Object>> queryWithPartition(QueryElement queryElement, MemoryPartition routeMemoryPartition) {
        boolean isFindAll = queryElement.isSelectAll();
        boolean allColumn = queryElement.isAllColumn();
        boolean withoutCondition = queryElement.isWithoutCondition();
        Set<String> needColumnNames = queryElement.getNeedColumnNames();
        ConditionElement conditionElement = queryElement.getConditionElement();



    }
}
