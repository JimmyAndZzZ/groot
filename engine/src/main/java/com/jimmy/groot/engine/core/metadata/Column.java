package com.jimmy.groot.engine.core.metadata;

import com.jimmy.groot.sql.enums.ColumnTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Column implements Serializable {

    private String name;

    private ColumnTypeEnum columnType;
}
