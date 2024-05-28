package com.jimmy.groot.engine.metadata;

import lombok.Getter;

import java.io.Serializable;
import java.util.List;

public class Table implements Serializable {

    @Getter
    private String schema;

    @Getter
    private String tableName;

    private List<Column> columns;
}
