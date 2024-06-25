package com.jimmy.groot.engine.data.lsm;

import com.jimmy.groot.engine.enums.TableDataTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class TableData implements Serializable {

    private String key;

    private String value;

    private TableDataTypeEnum tableDataType;

    public TableData(String key, String value, TableDataTypeEnum tableDataType) {
        this.key = key;
        this.value = value;
        this.tableDataType = tableDataType;
    }

    public TableData() {
        
    }
}
