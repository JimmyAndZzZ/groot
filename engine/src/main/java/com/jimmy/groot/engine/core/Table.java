package com.jimmy.groot.engine.core;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Table implements Serializable {

    private String schema;

    private String tableName;

    private Map<String, Partition> partitionMap = Maps.newHashMap();

}
