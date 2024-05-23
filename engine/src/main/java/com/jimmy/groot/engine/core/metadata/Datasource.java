package com.jimmy.groot.engine.core.metadata;

import com.google.common.collect.Maps;
import com.jimmy.groot.engine.core.metadata.Schema;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Datasource implements Serializable {

    private Map<String, Schema> schemaMap = Maps.newHashMap();
}
