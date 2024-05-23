package com.jimmy.groot.engine.core.other;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@Data
public class ConditionPart implements Serializable {

    private String fullExpression;

    private String uniqueExpression;

    private String partitionExpression;

    private Set<String> uniqueCodes = Sets.newHashSet();

    private Set<String> partitionCodes = Sets.newHashSet();

    private Map<String, Object> conditionArgument = Maps.newHashMap();
}
