package com.jimmy.groot.engine.core;

import com.google.common.collect.Maps;
import com.jimmy.groot.sql.exception.EngineException;

import java.io.Serializable;
import java.util.Map;

public class Table implements Serializable {

    private String schema;

    private String tableName;

    private Map<String, Partition> partitionMap = Maps.newHashMap();



}
