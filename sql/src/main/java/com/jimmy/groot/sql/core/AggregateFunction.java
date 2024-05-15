package com.jimmy.groot.sql.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class AggregateFunction implements Serializable {

    private AggregateEnum aggregateType;

    private String column;

    private String alias;

    private Boolean isIncludeAlias = false;
}
