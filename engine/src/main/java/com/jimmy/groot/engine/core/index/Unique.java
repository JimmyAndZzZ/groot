package com.jimmy.groot.engine.core.index;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Unique implements Serializable {

    private String code;

    private Map<String, Object> uniqueData = Maps.newHashMap();
}
