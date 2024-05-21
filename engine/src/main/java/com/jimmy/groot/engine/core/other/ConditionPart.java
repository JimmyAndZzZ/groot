package com.jimmy.groot.engine.core.other;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@Data
public class ConditionPart implements Serializable {

    private String expression;

    private Set<String> mayNeedIndexFields = Sets.newHashSet();

    private Map<String, Object> conditionArgument = Maps.newHashMap();
}
