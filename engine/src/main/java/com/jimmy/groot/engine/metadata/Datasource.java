package com.jimmy.groot.engine.metadata;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Datasource implements Serializable {

    private Map<String, Schema> schemaMap = Maps.newHashMap();
}
