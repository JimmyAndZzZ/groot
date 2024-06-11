package com.jimmy.groot.engine.data.other;

import com.google.common.collect.Maps;
import com.googlecode.aviator.Expression;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@Data
public class ConditionExpression implements Serializable {

    private Expression expression;

    private Map<String, Object> conditionArgument = Maps.newHashMap();

    private Map<String, Set<Object>> keyConditionValue = Maps.newHashMap();
}
