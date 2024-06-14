package com.jimmy.groot.engine.data.other;

import com.google.common.collect.Maps;
import com.googlecode.aviator.Expression;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

@Data
public class ConditionExpression implements Serializable {

    private Expression otherExpression;

    private Expression uniqueExpression;

    private Map<String, Object> otherConditionArgument = Maps.newHashMap();

    private Map<String, Object> uniqueConditionArgument = Maps.newHashMap();
}
