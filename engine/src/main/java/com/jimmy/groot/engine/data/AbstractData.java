package com.jimmy.groot.engine.data;

import com.google.common.collect.Maps;
import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.base.Data;
import com.jimmy.groot.engine.convert.DateConvert;
import com.jimmy.groot.engine.convert.DefaultConvert;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.engine.metadata.Index;
import com.jimmy.groot.platform.other.Assert;
import com.jimmy.groot.sql.enums.ColumnTypeEnum;

import java.util.List;
import java.util.Map;

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
     * 获取转换器
     *
     * @param columnType
     * @return
     */
    protected Convert<?> getConvert(ColumnTypeEnum columnType) {
        Convert<?> convert = converts.get(columnType);
        return convert != null ? convert : DefaultConvert.getInstance();
    }
}
