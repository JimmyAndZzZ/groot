package com.jimmy.groot.engine.core;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class UniqueConstraint implements Serializable {

    private String code;

    private Map<String, Object> uniqueData = Maps.newHashMap();
}
