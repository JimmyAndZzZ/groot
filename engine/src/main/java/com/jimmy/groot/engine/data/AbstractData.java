package com.jimmy.groot.engine.data;

import cn.hutool.crypto.SecureUtil;
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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
