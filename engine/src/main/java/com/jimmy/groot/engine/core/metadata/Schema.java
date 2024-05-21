package com.jimmy.groot.engine.core.metadata;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Schema implements Serializable {

    private String schema;

    private Map<String, Table> tableMap = Maps.newHashMap();

}
