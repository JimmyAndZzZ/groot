package com.jimmy.groot.engine.data;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.base.Data;
import com.jimmy.groot.engine.convert.DateConvert;
import com.jimmy.groot.engine.convert.DefaultConvert;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Index;
import com.jimmy.groot.platform.other.Assert;
import com.jimmy.groot.sql.enums.ColumnTypeEnum;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;

public abstract class AbstractData implements Data {

    private final Map<ColumnTypeEnum, Convert<?>> converts = Maps.newHashMap();

    protected Index uniqueIndex;

    protected Index partitionIndex;

    protected List<Column> columns;

    public AbstractData(List<Column> columns) {
        this.columns = columns;
        this.converts.put(ColumnTypeEnum.DATE, new DateConvert());

        this.columns = columns;
        this.uniqueIndex = new Index();
        this.partitionIndex = new Index();

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


    /**
     * 获取分区键key
     *
     * @param doc
     * @return
     */
    protected IndexData getPartitionData(Map<String, Object> doc) {
        Map<String, Object> partitionData = Maps.newHashMap();

        for (String column : partitionIndex.getColumns()) {
            Object o = doc.get(column);
            Assert.notNull(o, "分区键值为空，字段名:" + column);
            partitionData.put(column, o);
        }

        return new IndexData(this.getKey(partitionData), partitionData);
    }

    /**
     * 获取唯一键key
     *
     * @param doc
     * @return
     */
    protected IndexData getUniqueData(Map<String, Object> doc) {
        Map<String, Object> uniqueData = Maps.newHashMap();

        for (String column : uniqueIndex.getColumns()) {
            Object o = doc.get(column);
            Assert.notNull(o, "主键值为空，字段名:" + column);
            uniqueData.put(column, o);
        }

        return new IndexData(this.getKey(uniqueData), uniqueData);
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

    @Getter
    protected static class IndexData implements Serializable {

        private final String key;

        private final Map<String, Object> data;

        public IndexData(String key, Map<String, Object> data) {
            this.key = key;
            this.data = data;
        }
    }
}
